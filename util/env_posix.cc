// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <set>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

#define _NVMDEBUG
#define _USE_NVM
#ifdef _USE_NVM
#include <nv_map.h>
#include <c_io.h>
#endif

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
	return Status::IOError(context, strerror(err_number));
}

static int check_if_enable(const char *fname) {

	if( strstr(fname,".log") || strstr(fname,".ldb")
		|| strstr(fname, "MANIFEST") || strstr(fname, "CURRENT")
		|| strstr(fname, ".dbtmp"))
		return 1;
	else
		return 0;
}


#ifdef _USE_NVM

class NVMWriteFile : public WritableFile {
public:
	std::string contents_;

	virtual Status Close() { return Status::OK(); }
	virtual Status Flush() { return Status::OK(); }
	virtual Status Sync() { return Status::OK(); }
	virtual Status Append(const Slice& slice) {
		contents_.append(slice.data(), slice.size());
		return Status::OK();
	}
};

class NVMSequentialFile : public SequentialFile, public WritableFile {
public:
	Slice contents_;
	bool force_error_;
	bool returned_partial_;
	char nv_objname[255];
	unsigned int objid;
	void *base_address;
	size_t base_length;
	size_t nvmwriteoff;
	size_t nvmreadoff;
	pthread_mutex_t rd_mutex; //= PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t wr_mutex; //= PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t append_mutex; //= PTHREAD_MUTEX_INITIALIZER;
	Status s;


	NVMSequentialFile(const std::string& fname, int readflag) :
		force_error_(false),
		returned_partial_(false)
	{
		strcpy(nv_objname, (char *)fname.c_str());
		nvmwriteoff = 0;
		nvmreadoff = 0;
  	    rd_mutex = PTHREAD_MUTEX_INITIALIZER;
	    wr_mutex = PTHREAD_MUTEX_INITIALIZER;
	    append_mutex = PTHREAD_MUTEX_INITIALIZER;
		//fprintf(stderr,"creating nv_objname %s\n",nv_objname);

		if(!readflag){
			unsigned long ref;
			base_length = 1024*1024*4;
			
			//base_address = nvalloc_(base_length, (char *)fname.c_str(), 0);
			pthread_mutex_lock( &wr_mutex );
			base_address = nvalloc_id(base_length, (char *)fname.c_str(), &objid);
			pthread_mutex_unlock( &wr_mutex );

			memset(base_address, 0, base_length);
#ifdef _NVMDEBUG
			fprintf(stderr,"creating nv_objname %s, address %lu, objid %u\n",
							nv_objname, (unsigned long)base_address, objid);
#endif
		}else{

			pthread_mutex_lock( &rd_mutex );
			base_address = nvread_len((char *)fname.c_str(),objid, &base_length);
			if(!base_address)
				nvmwriteoff = 0;
			nvmwriteoff = base_length;
			pthread_mutex_unlock( &rd_mutex );

#ifdef _NVMDEBUG
			fprintf(stderr,"nvread_len nv_objname %s, address %lu\n",nv_objname, (unsigned long)base_address);
			fprintf(stderr,"creating nvmwriteoff %u\n",nvmwriteoff);
#endif
		}
	}

	virtual Status Read(size_t n, Slice* result, char* scratch) {

		if(!nvmwriteoff) {
			//force_error_ = true;
			returned_partial_ = true;
			return s;
		}

		if( nvmreadoff >= nvmwriteoff) {
			n = 0;
			//force_error_ = true;
			returned_partial_ = true;
			return s;
		}

		if( nvmreadoff + n > nvmwriteoff) {
			n = nvmwriteoff - nvmreadoff;
			returned_partial_ = true;
		}

		*result = Slice(reinterpret_cast<char*>(base_address + nvmreadoff ), n);
		nvmreadoff= nvmreadoff +n;
		return Status::OK();

	}

	virtual Status Close() { 

		//pthread_mutex_lock( &wr_mutex );	
		#if 0
		if(objid) {
			nvcommitsz_id(objid, nvmwriteoff);
		}else {
			nvcommitsz(nv_objname, nvmwriteoff);
		}
		#endif
		//pthread_mutex_unlock( &wr_mutex );
		return Status::OK(); 
	}

	virtual Status Flush() { 

		#if 1
		pthread_mutex_lock( &wr_mutex );
		#endif
		if(objid) {
			nvcommitsz_id(objid, nvmwriteoff);
		}else {
			nvcommitsz(nv_objname, nvmwriteoff);
		}
		#if 1
		pthread_mutex_unlock( &wr_mutex );
		#endif	
		return Status::OK(); 
	}

	virtual Status Sync() { 

		#if 0
		pthread_mutex_lock( &wr_mutex );
			
		if(objid) {
			nvcommitsz_id(objid, nvmwriteoff);
		}else {
			nvcommitsz(nv_objname, nvmwriteoff);
		}
		pthread_mutex_unlock( &wr_mutex );
		#endif	
		return Status::OK(); 
	}

	virtual Status Append(const Slice& slice) {

		unsigned long ptr;

		size_t size =  slice.size();

		memcpy(base_address+nvmwriteoff, slice.data(), size);
		nvmwriteoff = nvmwriteoff + size;
		//fprintf(stderr,"nvm write obj %s %lu nvmoffset\n", nv_objname,  nvmwriteoff);
		pthread_mutex_lock( &append_mutex );
		if(objid) {
			nvcommitsz_id(objid, nvmwriteoff);
		}else {
			nvcommitsz(nv_objname, nvmwriteoff);
		}
		pthread_mutex_unlock( &append_mutex );
		//nvsync(base_address+nvmwriteoff, size); 


		return Status::OK();
	}

	virtual Status Skip(uint64_t n) {
		if (n > contents_.size()) {
			contents_.clear();
			return Status::NotFound("in-memory file skipepd past end");
		}
		contents_.remove_prefix(n);
		return Status::OK();
	}
};
#endif

class PosixSequentialFile: public SequentialFile {

private:
	std::string filename_;
	FILE* file_;
#ifdef _USE_NVM
	//void *pvptr=NULL;
	char nv_objname[255];
	NVMSequentialFile *nvmseqfile;
#endif

public:
	PosixSequentialFile(const std::string& fname, FILE* f, int readflag)
: filename_(fname), file_(f) {

#ifdef _USE_NVM
		//if(check_if_enable(filename_.c_str())) {
		nvmseqfile = new NVMSequentialFile(fname, readflag);
		//}
#endif
	}
	virtual ~PosixSequentialFile() {
		if(file_)
			fclose(file_);
	}

	virtual Status Read(size_t n, Slice* result, char* scratch) {
		Status s;
		size_t r;

#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str())) {
			if(nvmseqfile) {
				Slice* temp = result;
				s =nvmseqfile->Read(n, result, scratch);
				if(nvmseqfile->force_error_ == true)
					s = IOError(filename_, errno);

				return s;

			}else {
				fprintf(stderr,"nvmseqfile is NULL\n");
			}
			return s;
		}else {
			r= fread_unlocked(scratch, 1, n, file_);
		}
#else

#ifdef _NVMDEBUG
		if(check_if_enable(filename_.c_str()))
			fprintf(stderr, "reading offset fname%s: %u \n", filename_.c_str(), ftell(file_));
#endif

		r= fread_unlocked(scratch, 1, n, file_);
#endif
		*result = Slice(scratch, r);
		if (r < n) {
			if (feof(file_)) {
				// We leave status as ok if we hit the end of the file
			} else {
				// A partial read with an error: return a non-ok status
				s = IOError(filename_, errno);
			}
		}
		return s;
	}

	virtual Status Skip(uint64_t n) {

		if (fseek(file_, n, SEEK_CUR)) {
			return IOError(filename_, errno);
		}
		return Status::OK();
	}
};


#ifdef _USE_NVM
class NVMRandomNVMFile : public RandomAccessFile {
public:
	Slice contents_;
	bool force_error_;
	bool returned_partial_;
	char nv_objname[255];
	void *pvptr;
	void *base_address;
	size_t base_length;
	size_t nvmoffset;

	NVMRandomNVMFile(const std::string& fname) :
		force_error_(false),
		returned_partial_(false)
	{
		strcpy(nv_objname, (char *)fname.c_str());
		//fprintf(stderr,"NVMRandomNVMFile nv_objname %s\n",nv_objname);
		base_address = nvread_len((char *)fname.c_str(), 0, &base_length);
		nvmoffset = base_length;
		pvptr=base_address;
	}

	virtual Status Read(uint64_t offset, size_t n, Slice* result,
			char* scratch) const {

		*result = Slice(reinterpret_cast<char*>(base_address+offset), n);
		//fprintf(stderr,"NVMRandomNVMFile read address %lu offset %u\n", (unsigned long)base_address, offset);
		return Status::OK();
	}

	virtual Status Skip(uint64_t n) {
		if (n > contents_.size()) {
			contents_.clear();
			return Status::NotFound("in-memory file skipepd past end");
		}
		contents_.remove_prefix(n);
		return Status::OK();
	}
};
#endif


// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
private:
	std::string filename_;
	int fd_;

#ifdef _USE_NVM
	NVMRandomNVMFile *nvmrandfile;
#endif


public:
	PosixRandomAccessFile(const std::string& fname, int fd, int readflag)
: filename_(fname), fd_(fd) {

#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str())) {
			nvmrandfile = new NVMRandomNVMFile(fname);
		}
#endif
	}

	virtual ~PosixRandomAccessFile() { close(fd_); }

	virtual Status Read(uint64_t offset, size_t n, Slice* result,
			char* scratch) const {
		Status s;

#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str())) {
			s = nvmrandfile->Read(offset, n, result, scratch);
		}else {
			ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
			*result = Slice(scratch, (r < 0) ? 0 : r);
			if (r < 0) {
				s = IOError(filename_, errno);
			}
		}
#else
		ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
		*result = Slice(scratch, (r < 0) ? 0 : r);
		if (r < 0) {
			// An error: return a non-ok status
			s = IOError(filename_, errno);
		}
#endif
		return s;
	}
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
public:
	// Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
	MmapLimiter() {
		SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
	}

	// If another mmap slot is available, acquire it and return true.
	// Else return false.
	bool Acquire() {
		if (GetAllowed() <= 0) {
			return false;
		}
		MutexLock l(&mu_);
		intptr_t x = GetAllowed();
		if (x <= 0) {
			return false;
		} else {
			SetAllowed(x - 1);
			return true;
		}
	}

	// Release a slot acquired by a previous call to Acquire() that returned true.
	void Release() {
		MutexLock l(&mu_);
		SetAllowed(GetAllowed() + 1);
	}

private:
	port::Mutex mu_;
	port::AtomicPointer allowed_;

	intptr_t GetAllowed() const {
		return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
	}

	// REQUIRES: mu_ must be held
	void SetAllowed(intptr_t v) {
		allowed_.Release_Store(reinterpret_cast<void*>(v));
	}

	MmapLimiter(const MmapLimiter&);
	void operator=(const MmapLimiter&);
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
private:
	std::string filename_;
	void* mmapped_region_;
	size_t length_;
	MmapLimiter* limiter_;

#ifdef _USE_NVM
	NVMRandomNVMFile *nvmrandfile;
#endif


public:
	// base[0,length-1] contains the mmapped contents of the file.
	PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
			MmapLimiter* limiter)
: filename_(fname), mmapped_region_(base), length_(length),
  limiter_(limiter) {

#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str())) {
			nvmrandfile = new NVMRandomNVMFile(fname);
		}
#endif
	}

	virtual ~PosixMmapReadableFile() {
		munmap(mmapped_region_, length_);
		limiter_->Release();
	}

	virtual Status Read(uint64_t offset, size_t n, Slice* result,
			char* scratch) const {

		Status s;
#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str())) {
			s = nvmrandfile->Read(offset, n, result, scratch);
		}
		return s;
#else
		if (offset + n > length_) {
			*result = Slice();
			s = IOError(filename_, EINVAL);
		} else {
			*result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
		}
#endif
		//assert(0);
		return s;
	}
};

class PosixWritableFile : public WritableFile {
private:
	std::string filename_;
	FILE* file_;

#ifdef _USE_NVM
	NVMSequentialFile *nvmwritefile;
#endif

public:
	PosixWritableFile(const std::string& fname, FILE* f)
: filename_(fname), file_(f) {

#ifdef _USE_NVM
		//if(check_if_enable(fname.c_str())) {
		nvmwritefile = new NVMSequentialFile(fname, 0);
		//}
#endif
	}

	~PosixWritableFile() {

		if (file_ != NULL) {
			// Ignoring any potential errors
			fclose(file_);
		}
	}

	virtual Status Append(const Slice& data) {

#ifdef _USE_NVM
		size_t r;

		if(check_if_enable(filename_.c_str())) {
			r = data.size();
			nvmwritefile->Append(data);
		}else {
			r = fwrite_unlocked(data.data(), 1, data.size(), file_);
		}
		if (r != data.size()) {
			return IOError(filename_, errno);
		}
#else

#ifdef _NVMDEBUG
		if(check_if_enable(filename_.c_str()))
			fprintf(stderr, "writing offset fname%s: %u \n", filename_.c_str(), ftell(file_));
#endif

		size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
		if (r != data.size()) {
			return IOError(filename_, errno);
		}
#endif
		return Status::OK();
	}

	virtual Status Close() {
		Status result;
#ifdef _USE_NVM
		if(check_if_enable(filename_.c_str()))
		return result;
#endif
		if (fclose(file_) != 0) {
			result = IOError(filename_, errno);
		}

		file_ = NULL;
		return result;
	}

	virtual Status Flush() {

#ifdef _USE_NVM
		//commit data here
		//DeleteFile(filename_);
		if(check_if_enable(filename_.c_str())) {
			nvmwritefile->Flush();
		}
		return Status::OK();
#else
		if (fflush_unlocked(file_) != 0) {
			return IOError(filename_, errno);
		}
#endif
		return Status::OK();
	}

	Status SyncDirIfManifest() {
		const char* f = filename_.c_str();
		const char* sep = strrchr(f, '/');
		Slice basename;
		std::string dir;
		if (sep == NULL) {
			dir = ".";
			basename = f;
		} else {
			dir = std::string(f, sep - f);
			basename = sep + 1;
		}
		Status s;
		if (basename.starts_with("MANIFEST")) {
			int fd = open(dir.c_str(), O_RDONLY);
			if (fd < 0) {
				s = IOError(dir, errno);
			} else {
				if (fsync(fd) < 0) {
					s = IOError(dir, errno);
				}
				close(fd);
			}
		}
		return s;
	}

	virtual Status Sync() {

		// Ensure new files referred to by the manifest are in the filesystem.
		Status s = SyncDirIfManifest();
		if (!s.ok()) {
			return s;
		}
#ifdef _USE_NVM
		//sync data here
		if(check_if_enable(filename_.c_str())) {
			nvmwritefile->Sync();
		}
		return s;
#else
		if (fflush_unlocked(file_) != 0 ||
				fdatasync(fileno(file_)) != 0) {
			s = Status::IOError(filename_, strerror(errno));
		}
		return s;
#endif
	}
};

static int LockOrUnlock(int fd, bool lock) {
	errno = 0;
	struct flock f;
	memset(&f, 0, sizeof(f));
	f.l_type = (lock ? F_WRLCK : F_UNLCK);
	f.l_whence = SEEK_SET;
	f.l_start = 0;
	f.l_len = 0;        // Lock/unlock entire file
	return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
public:
	int fd_;
	std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable {
private:
	port::Mutex mu_;
	std::set<std::string> locked_files_;
public:
	bool Insert(const std::string& fname) {
		MutexLock l(&mu_);
		return locked_files_.insert(fname).second;
	}
	void Remove(const std::string& fname) {
		MutexLock l(&mu_);
		locked_files_.erase(fname);
	}
};

class PosixEnv : public Env {
public:
	PosixEnv();
	virtual ~PosixEnv() {
		char msg[] = "Destroying Env::Default()\n";
		fwrite(msg, 1, sizeof(msg), stderr);
		abort();
	}

	virtual Status NewSequentialFile(const std::string& fname,
			SequentialFile** result) {

#ifdef _USE_NVM
		//if(check_if_enable(fname.c_str())){
		*result = new PosixSequentialFile(fname, NULL, 1);
		//return Status::OK();
		//}
		return Status::OK();
#endif

		FILE* f = fopen(fname.c_str(), "r");
		if (f == NULL) {
			*result = NULL;
			return IOError(fname, errno);
		} else {
			*result = new PosixSequentialFile(fname, f, 1);
			return Status::OK();
		}
	}

	virtual Status NewRandomAccessFile(const std::string& fname,
			RandomAccessFile** result) {

		*result = NULL;
		Status s;

#ifdef _USE_NVM
		//if(check_if_enable(fname.c_str())){
		*result = new PosixRandomAccessFile(fname, -1, 0);
		return s;
		//}
#endif
		int fd = open(fname.c_str(), O_RDONLY);
		if (fd < 0) {
			perror("error opening file");
			s = IOError(fname, errno);
		} else if (mmap_limit_.Acquire()) {
			uint64_t size;
			s = GetFileSize(fname, &size);
			if (s.ok()) {
				void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
				if (base != MAP_FAILED) {
					*result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
				} else {
					s = IOError(fname, errno);
				}
			}
			close(fd);
			if (!s.ok()) {
				mmap_limit_.Release();
			}
		} else {
			*result = new PosixRandomAccessFile(fname, fd, 0);
		}
		return s;
	}

	virtual Status NewWritableFile(const std::string& fname,
			WritableFile** result) {
		Status s;

#ifdef _USE_NVM
		if(check_if_enable(fname.c_str())) {
			//FILE* f = fopen(fname.c_str(), "w");
			//*result =  new PosixWritableFile(fname, NULL);
			//return s;
		}
#endif

#ifdef _NVMDEBUG
		if(check_if_enable(fname.c_str()))
			fprintf(stderr,"creating file %s\n", fname.c_str());
#endif

		FILE* f = fopen(fname.c_str(), "w");
		if (f == NULL) {
			*result = NULL;
			s = IOError(fname, errno);
		} else {

			//fprintf(stderr,"creating file 1%s\n", fname.c_str());
			*result = new PosixWritableFile(fname, f);
		}
		return s;
	}

	virtual bool FileExists(const std::string& fname) {
		return access(fname.c_str(), F_OK) == 0;
	}

	virtual Status GetChildren(const std::string& dir,
			std::vector<std::string>* result) {
		result->clear();

#ifdef _NVMDEBUG
		fprintf(stderr, "----------------------------\n");
#endif

#ifdef _USE_NVM
		int count=0, i=0;

		char **list = get_object_name_list(0, &count);

		for(i=0; i< count; i++){
			list[i] = list[i] + strlen(dir.c_str()) + 1;
			std::string entry(list[i]);
			result->push_back(entry);
#ifdef _NVMDEBUG
			fprintf(stderr, "GetChildren1: filename %s\n", entry.c_str());
#endif
		}

#ifdef _NVMDEBUG
		fprintf(stderr, "----------------------------\n");
#endif

#else
		DIR* d = opendir(dir.c_str());
		if (d == NULL) {
			return IOError(dir, errno);
		}

		struct dirent* entry;
		while ((entry = readdir(d)) != NULL) {
			result->push_back(entry->d_name);
#ifdef _NVMDEBUG
			fprintf(stderr, "GetChildren: filename %s\n", entry->d_name);
#endif
		}
#ifdef _NVMDEBUG
		fprintf(stderr, "----------------------------\n");
#endif
		closedir(d);
#endif

		return Status::OK();
	}

	virtual Status DeleteFile(const std::string& fname) {
		Status result;

#ifdef _NVMDEBUG
		fprintf(stderr,"DeleteFile filename %s\n", fname.c_str());
#endif

#ifdef _USE_NVM
		//if(check_if_enable(fname.c_str())) {
		nvdelete((char *)fname.c_str(), 0);
		//}
#endif
		if (unlink(fname.c_str()) != 0) {
			result = IOError(fname, errno);
		}
		return result;
	}

	virtual Status CreateDir(const std::string& name) {
		Status result;
		if (mkdir(name.c_str(), 0755) != 0) {
			result = IOError(name, errno);
		}
		return result;
	}

	virtual Status DeleteDir(const std::string& name) {
		Status result;
		if (rmdir(name.c_str()) != 0) {
			result = IOError(name, errno);
		}
		return result;
	}

	virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
		Status s;
		struct stat sbuf;
		if (stat(fname.c_str(), &sbuf) != 0) {
			*size = 0;
			s = IOError(fname, errno);
		} else {
			*size = sbuf.st_size;
		}
		return s;
	}

	virtual Status RenameFile(const std::string& src, const std::string& target) {
		Status result;



#ifdef _USE_NVM
		if(check_if_enable(src.c_str()) || check_if_enable(target.c_str()) ) {

			//"target.c_str() %s \n",src.c_str(), target.c_str());
			nv_renameobj((char *)src.c_str(), (char *)target.c_str());
			//return result;
		}

		//nv_renameobj((char *)src.c_str(), (char *)target.c_str());

		//if(check_if_enable(src.c_str())) {
		//fprintf(stderr,"RenameFile filename %s\n", src.c_str());
		//return result;
		//}
#endif

		if (rename(src.c_str(), target.c_str()) != 0) {
			result = IOError(src, errno);
		}
		return result;
	}

	virtual Status LockFile(const std::string& fname, FileLock** lock) {
		*lock = NULL;
		Status result;

#ifdef _USE_NVM
		//if(check_if_enable(fname.c_str())) {
		//return result;
		//}
#endif

		int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
		if (fd < 0) {
			result = IOError(fname, errno);
		} else if (!locks_.Insert(fname)) {
			close(fd);
			result = Status::IOError("lock " + fname, "already held by process");
		} else if (LockOrUnlock(fd, true) == -1) {
			result = IOError("lock " + fname, errno);
			close(fd);
			locks_.Remove(fname);
		} else {
			PosixFileLock* my_lock = new PosixFileLock;
			my_lock->fd_ = fd;
			my_lock->name_ = fname;
			*lock = my_lock;
		}
		return result;
	}

	virtual Status UnlockFile(FileLock* lock) {
		PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
		Status result;


		if (LockOrUnlock(my_lock->fd_, false) == -1) {
			result = IOError("unlock", errno);
		}
		locks_.Remove(my_lock->name_);
		close(my_lock->fd_);
		delete my_lock;
		return result;
	}

	virtual void Schedule(void (*function)(void*), void* arg);

	virtual void StartThread(void (*function)(void* arg), void* arg);

	virtual Status GetTestDirectory(std::string* result) {
		const char* env = getenv("TEST_TMPDIR");
		if (env && env[0] != '\0') {
			*result = env;
		} else {
			char buf[100];
			snprintf(buf, sizeof(buf), "/tmp/ramdisk/leveldbtest-1000", int(geteuid()));
			*result = buf;
		}
		// Directory may already exist
		CreateDir(*result);
		return Status::OK();
	}

	static uint64_t gettid() {
		pthread_t tid = pthread_self();
		uint64_t thread_id = 0;
		memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
		return thread_id;
	}

	virtual Status NewLogger(const std::string& fname, Logger** result) {

		FILE* f = fopen(fname.c_str(), "w");
		if (f == NULL) {
			*result = NULL;
			return IOError(fname, errno);
		} else {
			*result = new PosixLogger(f, &PosixEnv::gettid);
			return Status::OK();
		}
	}

	virtual uint64_t NowMicros() {
		struct timeval tv;
		gettimeofday(&tv, NULL);
		return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
	}

	virtual void SleepForMicroseconds(int micros) {
		usleep(micros);
	}

private:
	void PthreadCall(const char* label, int result) {
		if (result != 0) {
			fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
			abort();
		}
	}

	// BGThread() is the body of the background thread
	void BGThread();
	static void* BGThreadWrapper(void* arg) {
		reinterpret_cast<PosixEnv*>(arg)->BGThread();
		return NULL;
	}

	pthread_mutex_t mu_;
	pthread_cond_t bgsignal_;
	pthread_t bgthread_;
	bool started_bgthread_;

	// Entry per Schedule() call
	struct BGItem { void* arg; void (*function)(void*); };
	typedef std::deque<BGItem> BGQueue;
	BGQueue queue_;

	PosixLockTable locks_;
	MmapLimiter mmap_limit_;
};

PosixEnv::PosixEnv() : started_bgthread_(false) {
	PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
	PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
	PthreadCall("lock", pthread_mutex_lock(&mu_));

	// Start background thread if necessary
	if (!started_bgthread_) {
		started_bgthread_ = true;
		PthreadCall(
				"create thread",
				pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
	}

	// If the queue is currently empty, the background thread may currently be
	// waiting.
	if (queue_.empty()) {
		PthreadCall("signal", pthread_cond_signal(&bgsignal_));
	}

	// Add to priority queue
	queue_.push_back(BGItem());
	queue_.back().function = function;
	queue_.back().arg = arg;

	PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
	while (true) {
		// Wait until there is an item that is ready to run
		PthreadCall("lock", pthread_mutex_lock(&mu_));
		while (queue_.empty()) {
			PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
		}

		void (*function)(void*) = queue_.front().function;
		void* arg = queue_.front().arg;
		queue_.pop_front();

		PthreadCall("unlock", pthread_mutex_unlock(&mu_));
		(*function)(arg);
	}
}

namespace {
struct StartThreadState {
	void (*user_function)(void*);
	void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
	StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
	state->user_function(state->arg);
	delete state;
	return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
	pthread_t t;
	StartThreadState* state = new StartThreadState;
	state->user_function = function;
	state->arg = arg;
	PthreadCall("start thread",
			pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
	pthread_once(&once, InitDefaultEnv);
	return default_env;
}

}  // namespace leveldb
