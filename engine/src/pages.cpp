/*
 * The pages store's disk (ch_table_engines.md §2.9): what is being written is
 * a DiskLocal's, as in the files store; the files of committed parts are in
 * the relation's pages, found by their manifests and read through the
 * caller's read_page, a page at a time, on the calling thread.  An insert's
 * part may instead be written into memory (VC18): its directory and files
 * kept here, for the caller to copy into pages without temporary files.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadPipeline.h>
#include <IO/WriteBufferFromFileBase.h>

#include <filesystem>

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
}

namespace fs = std::filesystem;

namespace chte
{

namespace
{

/// A path as the manifests key it: relative to the disk, no trailing '/', no "./"
String normalize(const String & path)
{
    String p = fs::path(path).lexically_normal().string();
    while (p.size() > 1 && p.back() == '/')
        p.pop_back();
    if (p == ".")
        p.clear();
    return p;
}

class ReadBufferFromPages final : public ReadBufferFromFileBase
{
public:
    ReadBufferFromPages(const PagesDisk & disk_, String name_, PagesDisk::File file_, size_t buf_size)
        : ReadBufferFromFileBase(std::max<size_t>(buf_size, disk_.pagePayload()), nullptr, 0, file_.size)
        , disk(disk_)
        , name(std::move(name_))
        , file(file_)
        , page(disk_.pagePayload())
    {
    }

    std::string getFileName() const override { return name; }

    off_t getPosition() override { return static_cast<off_t>(file_offset - available()); }

    off_t seek(off_t off, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "only SEEK_SET is supported reading {}", name);
        UInt64 target = static_cast<UInt64>(off);
        UInt64 buffer_start = file_offset - working_buffer.size();
        if (!working_buffer.empty() && target >= buffer_start && target <= file_offset)
        {
            pos = working_buffer.begin() + (target - buffer_start);
            return off;
        }
        file_offset = target;
        resetWorkingBuffer();
        return off;
    }

    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        if (file_offset >= file.size)
            return false;
        const UInt32 payload = disk.pagePayload();
        size_t n = std::min<UInt64>(internal_buffer.size(), file.size - file_offset);
        size_t done = 0;
        while (done < n)
        {
            UInt64 at = file_offset + done;
            UInt32 index = static_cast<UInt32>(at / payload);
            size_t in_page = at % payload;
            size_t chunk = std::min<size_t>(payload - in_page, n - done);

            if (index >= file.nblocks)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "{}: offset {} past its {} pages", name, at, file.nblocks);
            if (in_page == 0 && chunk == payload)
                disk.readPage(file.first_block + index, internal_buffer.begin() + done);
            else
            {
                disk.readPage(file.first_block + index, page.data());
                memcpy(internal_buffer.begin() + done, page.data() + in_page, chunk);
            }
            done += chunk;
        }
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        file_offset += n;
        return true;
    }

    const PagesDisk & disk;
    String name;
    PagesDisk::File file;
    std::vector<char> page;
    UInt64 file_offset = 0;
};

/// A file of a part in memory, being written (VC18)
class WriteBufferToMemory final : public WriteBufferFromFileBase
{
public:
    WriteBufferToMemory(String name_, std::shared_ptr<std::string> data_, size_t buf_size)
        : WriteBufferFromFileBase(std::max<size_t>(std::min<size_t>(buf_size, DBMS_DEFAULT_BUFFER_SIZE), 4096), nullptr, 0)
        , name(std::move(name_))
        , data(std::move(data_))
    {
    }

    void sync() override { next(); }
    std::string getFileName() const override { return name; }

private:
    void nextImpl() override
    {
        if (offset() > 0)
            data->append(working_buffer.begin(), offset());
    }

    String name;
    std::shared_ptr<std::string> data;
};

/// A file of a part in memory, read back
class ReadBufferFromMemoryFile final : public ReadBufferFromFileBase
{
public:
    ReadBufferFromMemoryFile(String name_, std::shared_ptr<const std::string> data_)
        : ReadBufferFromFileBase(0, nullptr, 0, data_->size())
        , name(std::move(name_))
        , data(std::move(data_))
    {
        char * begin = const_cast<char *>(data->data());
        working_buffer = Buffer(begin, begin + data->size());
        pos = working_buffer.begin();
    }

    std::string getFileName() const override { return name; }
    off_t getPosition() override { return static_cast<off_t>(pos - data->data()); }
    size_t getFileOffsetOfBufferEnd() const override { return data->size(); }

    off_t seek(off_t off, int whence) override
    {
        if (whence != SEEK_SET || off < 0 || static_cast<size_t>(off) > data->size())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "seeking {} to {}", name, off);
        char * begin = const_cast<char *>(data->data());
        working_buffer = Buffer(begin, begin + data->size());
        pos = begin + off;
        return off;
    }

private:
    bool nextImpl() override { return false; }

    String name;
    std::shared_ptr<const std::string> data;
};

class NamesIterator final : public IDirectoryIterator
{
public:
    NamesIterator(String dir_, std::vector<String> names_) : dir(std::move(dir_)), names(std::move(names_)) {}
    void next() override { ++i; }
    bool isValid() const override { return i < names.size(); }
    std::string path() const override { return (fs::path(dir) / names[i]).string(); }
    std::string name() const override
    {
        String n = names[i];
        if (!n.empty() && n.back() == '/')
            n.pop_back();
        return n;
    }

private:
    String dir;
    std::vector<String> names;
    size_t i = 0;
};

}

PagesDisk::PagesDisk(const String & name_, const String & path_, const chte_pages_ops & ops_)
    : DiskLocal(name_, path_)
    , ops(ops_)
{
}

void PagesDisk::attachPart(const String & part_dir, std::map<String, File> part_files)
{
    detachPart(part_dir);
    String root = normalize(part_dir);
    dirs.insert(root);
    for (auto & [name, f] : part_files)
    {
        String full = normalize(fs::path(root) / name);
        files[full] = f;
        /// a projection's subdirectory, and any between
        for (fs::path d = fs::path(full).parent_path(); !d.empty() && d.string().size() > root.size(); d = d.parent_path())
            dirs.insert(d.string());
    }
}

void PagesDisk::detachPart(const String & part_dir)
{
    String root = normalize(part_dir);
    String prefix = root + "/";
    for (auto it = files.lower_bound(prefix); it != files.end() && it->first.starts_with(prefix);)
        it = files.erase(it);
    for (auto it = dirs.lower_bound(root); it != dirs.end() && (*it == root || it->starts_with(prefix));)
        it = dirs.erase(it);
}

void PagesDisk::readPage(UInt32 block, char * dst) const
{
    if (ops.read_page(ops.ctx, block, dst) != 0)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "reading page {} of the relation failed", block);
}

bool PagesDisk::hasLocal(const String & path) const
{
    return DiskLocal::existsFileOrDirectory(path);
}

const PagesDisk::File * PagesDisk::findFile(const String & path) const
{
    auto it = files.find(normalize(path));
    return it == files.end() ? nullptr : &it->second;
}

std::vector<String> PagesDisk::entries(const String & dir) const
{
    String root = normalize(dir);
    String prefix = root + "/";
    std::set<String> names;
    for (auto it = files.lower_bound(prefix); it != files.end() && it->first.starts_with(prefix); ++it)
    {
        String rest = it->first.substr(prefix.size());
        auto slash = rest.find('/');
        names.insert(slash == String::npos ? rest : rest.substr(0, slash + 1));
    }
    return {names.begin(), names.end()};
}

bool PagesDisk::existsFile(const String & path) const
{
    if (inMemory(path))
    {
        std::lock_guard lock(memory_mutex);
        return memory_files.contains(memoryKey(path));
    }
    return findFile(path) != nullptr || DiskLocal::existsFile(path);
}

bool PagesDisk::existsDirectory(const String & path) const
{
    if (inMemory(path))
    {
        std::lock_guard lock(memory_mutex);
        return memory_dirs.contains(memoryKey(path));
    }
    return dirs.contains(normalize(path)) || DiskLocal::existsDirectory(path);
}

bool PagesDisk::existsFileOrDirectory(const String & path) const
{
    return existsFile(path) || existsDirectory(path);
}

size_t PagesDisk::getFileSize(const String & path) const
{
    if (inMemory(path))
    {
        std::lock_guard lock(memory_mutex);
        auto it = memory_files.find(memoryKey(path));
        if (it == memory_files.end())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "no file {} in memory", path);
        return it->second->size();
    }
    if (const auto * f = findFile(path))
        return f->size;
    return DiskLocal::getFileSize(path);
}

DirectoryIteratorPtr PagesDisk::iterateDirectory(const String & path) const
{
    if (inMemory(path))
        return std::make_unique<NamesIterator>(path, memoryEntries(path));
    if (dirs.contains(normalize(path)))
        return std::make_unique<NamesIterator>(path, entries(path));
    return DiskLocal::iterateDirectory(path);
}

void PagesDisk::listFiles(const String & path, std::vector<String> & file_names) const
{
    if (inMemory(path) || dirs.contains(normalize(path)))
    {
        file_names.clear();
        for (auto & n : inMemory(path) ? memoryEntries(path) : entries(path))
            file_names.push_back(n.back() == '/' ? n.substr(0, n.size() - 1) : n);
        return;
    }
    DiskLocal::listFiles(path, file_names);
}

Poco::Timestamp PagesDisk::getLastModified(const String & path) const
{
    if (inMemory(path))
        return Poco::Timestamp();
    if (!hasLocal(path) && (findFile(path) || dirs.contains(normalize(path))))
        return Poco::Timestamp::fromEpochTime(0);
    return DiskLocal::getLastModified(path);
}

time_t PagesDisk::getLastChanged(const String & path) const
{
    if (inMemory(path))
        return time(nullptr);
    if (!hasLocal(path) && (findFile(path) || dirs.contains(normalize(path))))
        return 0;
    return DiskLocal::getLastChanged(path);
}

void PagesDisk::prepareRead(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint,
                            ReadPipeline & pipeline) const
{
    if (inMemory(path))
    {
        String name = memoryKey(path);
        std::shared_ptr<const std::string> data;
        {
            std::lock_guard lock(memory_mutex);
            auto it = memory_files.find(name);
            if (it == memory_files.end())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "no file {} in memory", path);
            data = it->second;
        }
        StoredObject object(name, name, data->size());
        pipeline.setSource(
            [name, data](const StoredObject &, const ReadSettings &, bool, bool) -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return std::make_unique<ReadBufferFromMemoryFile>(name, data);
            },
            StoredObjects{object}, settings);
        return;
    }
    const File * f = findFile(path);
    if (f == nullptr)
    {
        DiskLocal::prepareRead(path, settings, read_hint, pipeline);
        return;
    }
    File file = *f;
    String name = normalize(path);
    StoredObject object(name, name, file.size);
    pipeline.setSource(
        [this, name, file](const StoredObject &, const ReadSettings & s, bool, bool) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return std::make_unique<ReadBufferFromPages>(*this, name, file, s.local_fs_settings.buffer_size);
        },
        StoredObjects{object}, settings);
}

/* ---- parts in memory (VC18) ---- */

String PagesDisk::memoryKey(const String & path) const
{
    /// a path of the disk's, relative, or absolute below its root, as a part's full path is
    const String & root = getPath();
    if (path.starts_with(root))
        return normalize(path.substr(root.size()));
    return normalize(path);
}

bool PagesDisk::inMemory(const String & path) const
{
    std::lock_guard lock(memory_mutex);
    if (memory_dirs.empty())
        return false;
    String p = memoryKey(path);
    for (fs::path d = p; !d.empty(); d = d.parent_path())
    {
        if (memory_dirs.contains(d.string()))
            return true;
        if (d == d.parent_path())
            break;
    }
    return false;
}

std::vector<String> PagesDisk::memoryEntries(const String & dir) const
{
    std::lock_guard lock(memory_mutex);
    String prefix = memoryKey(dir) + "/";
    std::set<String> names;
    for (auto it = memory_files.lower_bound(prefix); it != memory_files.end() && it->first.starts_with(prefix); ++it)
    {
        String rest = it->first.substr(prefix.size());
        auto slash = rest.find('/');
        names.insert(slash == String::npos ? rest : rest.substr(0, slash + 1));
    }
    for (auto it = memory_dirs.lower_bound(prefix); it != memory_dirs.end() && it->starts_with(prefix); ++it)
    {
        String rest = it->substr(prefix.size());
        if (!rest.empty() && rest.find('/') == String::npos)
            names.insert(rest + "/");
    }
    return {names.begin(), names.end()};
}

std::vector<std::pair<String, std::shared_ptr<const std::string>>> PagesDisk::memoryFiles(const String & dir) const
{
    std::lock_guard lock(memory_mutex);
    String prefix = memoryKey(dir) + "/";
    std::vector<std::pair<String, std::shared_ptr<const std::string>>> out;
    for (auto it = memory_files.lower_bound(prefix); it != memory_files.end() && it->first.starts_with(prefix); ++it)
        out.emplace_back(it->first.substr(prefix.size()), it->second);
    return out;
}

void PagesDisk::createDirectory(const String & path)
{
    createDirectories(path);
}

void PagesDisk::createDirectories(const String & path)
{
    String p = memoryKey(path);
    if (inMemory(p) || (memory_writes && fs::path(p).filename().string().starts_with("tmp_insert_")))
    {
        std::lock_guard lock(memory_mutex);
        memory_dirs.insert(p);
        return;
    }
    DiskLocal::createDirectories(path);
}

void PagesDisk::moveDirectory(const String & from_path, const String & to_path)
{
    if (!inMemory(from_path))
    {
        DiskLocal::moveDirectory(from_path, to_path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    String from = memoryKey(from_path);
    String to = memoryKey(to_path);
    std::set<String> new_dirs;
    for (auto it = memory_dirs.begin(); it != memory_dirs.end();)
        if (*it == from || it->starts_with(from + "/"))
        {
            new_dirs.insert(to + it->substr(from.size()));
            it = memory_dirs.erase(it);
        }
        else
            ++it;
    memory_dirs.insert(new_dirs.begin(), new_dirs.end());
    std::map<String, std::shared_ptr<std::string>> moved;
    for (auto it = memory_files.lower_bound(from + "/"); it != memory_files.end() && it->first.starts_with(from + "/");)
    {
        moved[to + it->first.substr(from.size())] = it->second;
        it = memory_files.erase(it);
    }
    memory_files.insert(moved.begin(), moved.end());
}

void PagesDisk::createFile(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::createFile(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    memory_files.try_emplace(memoryKey(path), std::make_shared<std::string>());
}

void PagesDisk::moveFile(const String & from_path, const String & to_path)
{
    if (!inMemory(from_path))
    {
        DiskLocal::moveFile(from_path, to_path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    auto node = memory_files.extract(memoryKey(from_path));
    if (node.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "no file {} in memory", from_path);
    node.key() = memoryKey(to_path);
    memory_files.insert_or_assign(node.key(), node.mapped());
}

void PagesDisk::replaceFile(const String & from_path, const String & to_path)
{
    moveFile(from_path, to_path);
}

std::unique_ptr<WriteBufferFromFileBase> PagesDisk::writeFile(const String & path, size_t buf_size, WriteMode mode,
                                                              const WriteSettings & settings)
{
    if (!inMemory(path))
        return DiskLocal::writeFile(path, buf_size, mode, settings);
    std::shared_ptr<std::string> data;
    {
        std::lock_guard lock(memory_mutex);
        auto & slot = memory_files[memoryKey(path)];
        if (!slot)
            slot = std::make_shared<std::string>();
        else if (mode == WriteMode::Rewrite)
        {
            /// a new file in its place: one handed out before keeps its bytes
            slot = std::make_shared<std::string>();
        }
        data = slot;
    }
    return std::make_unique<WriteBufferToMemory>(path, data, buf_size);
}

void PagesDisk::removeFile(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::removeFile(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    if (memory_files.erase(memoryKey(path)) == 0)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "no file {} in memory", path);
}

void PagesDisk::removeFileIfExists(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::removeFileIfExists(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    memory_files.erase(memoryKey(path));
}

void PagesDisk::removeDirectory(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::removeDirectory(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    memory_dirs.erase(memoryKey(path));
}

void PagesDisk::removeDirectoryIfExists(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::removeDirectoryIfExists(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    memory_dirs.erase(memoryKey(path));
}

void PagesDisk::removeRecursive(const String & path)
{
    if (!inMemory(path))
    {
        DiskLocal::removeRecursive(path);
        return;
    }
    std::lock_guard lock(memory_mutex);
    String p = memoryKey(path);
    memory_files.erase(p);
    for (auto it = memory_files.lower_bound(p + "/"); it != memory_files.end() && it->first.starts_with(p + "/");)
        it = memory_files.erase(it);
    for (auto it = memory_dirs.begin(); it != memory_dirs.end();)
        if (*it == p || it->starts_with(p + "/"))
            it = memory_dirs.erase(it);
        else
            ++it;
}

void PagesDisk::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    if (!inMemory(path))
        DiskLocal::setLastModified(path, timestamp);
}

void PagesDisk::setReadOnly(const String & path)
{
    if (!inMemory(path))
        DiskLocal::setReadOnly(path);
}

SyncGuardPtr PagesDisk::getDirectorySyncGuard(const String & path) const
{
    if (inMemory(path))
        return nullptr;
    return DiskLocal::getDirectorySyncGuard(path);
}

}
