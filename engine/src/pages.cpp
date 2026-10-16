/*
 * The pages store's disk (ch_table_engines.md §2.9): what is being written is
 * a DiskLocal's, as in the files store; the files of committed parts are in
 * the relation's pages, found by their manifests and read through the
 * caller's read_page, a page at a time, on the calling thread.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadPipeline.h>

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
    return findFile(path) != nullptr || DiskLocal::existsFile(path);
}

bool PagesDisk::existsDirectory(const String & path) const
{
    return dirs.contains(normalize(path)) || DiskLocal::existsDirectory(path);
}

bool PagesDisk::existsFileOrDirectory(const String & path) const
{
    return existsFile(path) || existsDirectory(path);
}

size_t PagesDisk::getFileSize(const String & path) const
{
    if (const auto * f = findFile(path))
        return f->size;
    return DiskLocal::getFileSize(path);
}

DirectoryIteratorPtr PagesDisk::iterateDirectory(const String & path) const
{
    if (dirs.contains(normalize(path)))
        return std::make_unique<NamesIterator>(path, entries(path));
    return DiskLocal::iterateDirectory(path);
}

void PagesDisk::listFiles(const String & path, std::vector<String> & file_names) const
{
    if (dirs.contains(normalize(path)))
    {
        file_names.clear();
        for (auto & n : entries(path))
            file_names.push_back(n.back() == '/' ? n.substr(0, n.size() - 1) : n);
        return;
    }
    DiskLocal::listFiles(path, file_names);
}

Poco::Timestamp PagesDisk::getLastModified(const String & path) const
{
    if (!hasLocal(path) && (findFile(path) || dirs.contains(normalize(path))))
        return Poco::Timestamp::fromEpochTime(0);
    return DiskLocal::getLastModified(path);
}

time_t PagesDisk::getLastChanged(const String & path) const
{
    if (!hasLocal(path) && (findFile(path) || dirs.contains(normalize(path))))
        return 0;
    return DiskLocal::getLastChanged(path);
}

void PagesDisk::prepareRead(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint,
                            ReadPipeline & pipeline) const
{
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

}
