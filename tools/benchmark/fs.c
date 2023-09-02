#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

#include "fs.h"

/* Minimum physical block size for direct I/O that we expect to detect. */
#define MIN_BLOCK_SIZE 512

/* Maximum physical block size for direct I/O that we expect to detect. */
#define MAX_BLOCK_SIZE (1024 * 1024) /* 1M */

static char *makeTempTemplate(const char *dir)
{
    char *path;
    path = malloc(strlen(dir) + strlen("/bench-XXXXXX") + 1);
    assert(path != NULL);
    sprintf(path, "%s/bench-XXXXXX", dir);
    return path;
}

int FsCreateTempFile(const char *dir, size_t size, char **path, int *fd)
{
    int dirfd;
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_DIRECT;
    int rv;

    *path = makeTempTemplate(dir);
    *fd = mkostemp(*path, flags);
    if (*fd == -1) {
        printf("mstemp '%s': %s\n", *path, strerror(errno));
        return -1;
    }

    rv = posix_fallocate(*fd, 0, (off_t)size);
    if (rv != 0) {
        errno = rv;
        printf("posix_fallocate: %s\n", strerror(errno));
        unlink(*path);
        return -1;
    }

    /* Sync the file and its directory. */
    rv = fsync(*fd);
    assert(rv == 0);

    dirfd = open(dir, O_RDONLY | O_DIRECTORY);
    assert(dirfd != -1);

    rv = fsync(dirfd);
    assert(rv == 0);

    close(dirfd);

    return 0;
}

int FsRemoveTempFile(char *path, int fd)
{
    int rv;

    rv = close(fd);
    if (rv != 0) {
        printf("close '%s': %s\n", path, strerror(errno));
        goto out;
    }
    rv = unlink(path);
    if (rv != 0) {
        printf("unlink '%s': %s\n", path, strerror(errno));
        goto out;
    }

out:
    free(path);
    return rv;
}

int FsCreateTempDir(const char *dir, char **path)
{
    *path = makeTempTemplate(dir);
    if (*path == NULL) {
        return -1;
    }
    *path = mkdtemp(*path);
    if (*path == NULL) {
        return -1;
    }
    return 0;
}

/* Wrapper around remove(), compatible with ntfw. */
static int dirRemoveFn(const char *path,
                       const struct stat *sbuf,
                       int type,
                       struct FTW *ftwb)
{
    (void)sbuf;
    (void)type;
    (void)ftwb;
    return remove(path);
}

int FsRemoveTempDir(char *path)
{
    int rv;
    rv = nftw(path, dirRemoveFn, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
    if (rv != 0) {
        return -1;
    }
    free(path);
    return 0;
}

int FsOpenBlockDevice(const char *dir, int *fd)
{
    *fd = open(dir, O_WRONLY | O_DIRECT);
    if (*fd == -1) {
        printf("open '%s': %s\n", dir, strerror(errno));
        return -1;
    }
    return 0;
}

/* Detect all suitable block size we can use to write to the underlying device
 * using direct I/O. */
static int detectSuitableBlockSizesForDirectIO(const char *dir,
                                               size_t **block_size,
                                               unsigned *n_block_size)
{
    char *path;
    int fd;
    size_t size;
    ssize_t rv;

    rv = FsCreateTempFile(dir, MAX_BLOCK_SIZE, &path, &fd);
    if (rv != 0) {
        unlink(path);
        return -1;
    }

    *block_size = NULL;
    *n_block_size = 0;

    for (size = MIN_BLOCK_SIZE; size <= MAX_BLOCK_SIZE; size *= 2) {
        struct iovec iov;
        iov.iov_len = size;
        iov.iov_base = aligned_alloc(iov.iov_len, iov.iov_len);
        assert(iov.iov_base != NULL);
        rv = pwritev2(fd, &iov, 1, 0, RWF_DSYNC | RWF_HIPRI);
        free(iov.iov_base);
        if (rv == -1) {
            assert(errno == EINVAL);
            continue; /* Try with a bigger buffer size */
        }
        assert((size_t)rv == size);
        *n_block_size += 1;
        *block_size = realloc(*block_size, *n_block_size * sizeof **block_size);
        assert(*block_size != NULL);
        (*block_size)[*n_block_size - 1] = size;
    }

    rv = FsRemoveTempFile(path, fd);
    if (rv != 0) {
        return -1;
    }

    return 0;
}

int FsFileExists(const char *dir, const char *name, bool *exists)
{
    char *path;
    struct stat st;
    int rv;

    path = malloc(strlen(dir) + strlen(name) + 1 + 1);
    assert(path != NULL);
    sprintf(path, "%s/%s", dir, name);

    rv = stat(path, &st);

    free(path);

    if (rv != 0) {
        if (errno != ENOENT) {
            return -1;
        }
        *exists = false;
    } else {
        *exists = true;
    }

    return 0;
}

int FsCheckDirectIO(const char *dir, size_t buf)
{
    size_t *block_size;
    unsigned n_block_size;
    unsigned i;
    int rv;

    rv = detectSuitableBlockSizesForDirectIO(dir, &block_size, &n_block_size);
    if (rv != 0) {
        goto err;
    }

    for (i = 0; i < n_block_size; i++) {
        if (block_size[i] == buf) {
            break;
        }
    }
    free(block_size);

    if (i == n_block_size) {
        goto err;
    }

    return 0;

err:
    return -1;
}
