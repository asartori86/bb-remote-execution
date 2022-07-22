package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

type blobAccessDirectoryFetcher struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessDirectoryFetcher creates a DirectoryFetcher that reads
// Directory objects from a BlobAccess based store.
func NewBlobAccessDirectoryFetcher(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) DirectoryFetcher {
	return &blobAccessDirectoryFetcher{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (df *blobAccessDirectoryFetcher) GetDirectory(ctx context.Context, digest digest.Digest) (*remoteexecution.Directory, error) {
	x := df.blobAccess.Get(ctx, digest)
	is_just := digest.NewHasher().Size() == justbuild.NewBlobHasher().Size()
	if is_just {
		data, err := x.ToByteSlice(df.maximumMessageSizeBytes)
		if err != nil {
			return nil, err
		}
		m, err := justbuild.ToDirectoryMessage(data)
		if err != nil {
			return nil, err
		}
		return m, nil
	}

	m, err := x.ToProto(&remoteexecution.Directory{}, df.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return m.(*remoteexecution.Directory), nil
}
