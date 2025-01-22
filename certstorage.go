package ddbcertstorage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/caddyserver/certmagic"
)

// Storage is a type that implements a key-value store with
// basic file system (folder path) semantics. Keys use the
// forward slash '/' to separate path components and have no
// leading or trailing slashes.
//
// A "prefix" of a key is defined on a component basis,
// e.g. "a" is a prefix of "a/b" but not "ab/c".
//
// A "file" is a key with a value associated with it.
//
// A "directory" is a key with no value, but which may be
// the prefix of other keys.
//
// Keys passed into Load and Store always have "file" semantics,
// whereas "directories" are only implicit by leading up to the
// file.
//
// The Load, Delete, List, and Stat methods should return
// fs.ErrNotExist if the key does not exist.
//
// Processes running in a cluster should use the same Storage
// value (with the same configuration) in order to share
// certificates and other TLS resources with the cluster.
//
// Implementations of Storage MUST be safe for concurrent use
// and honor context cancellations. Methods should block until
// their operation is complete; that is, Load() should always
// return the value from the last call to Store() for a given
// key, and concurrent calls to Store() should not corrupt a
// file.
//
// For simplicity, this is not a streaming API and is not
// suitable for very large files.
type Storage struct {
	table string
	ddb   *dynamodb.Client
}

var _ certmagic.Storage = (*Storage)(nil)

func New(table string) (*Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	return &Storage{
		table: table,
		ddb:   dynamodb.NewFromConfig(cfg),
	}, nil
}

// Store puts value at key. It creates the key if it does
// not exist and overwrites any existing value at this key.
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	keyParts := strings.Split(key, "/")
	for i := 0; i < len(keyParts)-1; i++ {
		item := &Item{
			Key:        strings.Join(keyParts[:i+1], "/"),
			Modified:   time.Now(),
			IsTerminal: false,
		}
		if _, err := s.ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &s.table,
			Item:      item.Item(),
		}); err != nil {
			return err
		}
	}
	item := &Item{
		Key:        key,
		Contents:   value,
		Modified:   time.Now(),
		Size:       int64(len(value)),
		IsTerminal: true,
	}
	_, err := s.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.table,
		Item:      item.Item(),
	})
	return err
}

// Load retrieves the value at key.
func (s *Storage) Load(ctx context.Context, key string) ([]byte, error) {
	output, err := s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(output.Item) == 0 {
		return nil, fs.ErrNotExist
	}
	var item Item
	if err := item.Load(output.Item); err != nil {
		return nil, err
	}
	return item.Contents, nil
}

// Delete deletes the named key. If the name is a
// directory (i.e. prefix of other keys), all keys
// prefixed by this key should be deleted. An error
// should be returned only if the key still exists
// when the method returns.
func (s *Storage) Delete(ctx context.Context, key string) error {
	output, err := s.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.table,
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{Value: key},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return err
	}
	if len(output.Attributes) == 0 {
		return nil
	}
	var item Item
	if err := item.Load(output.Attributes); err != nil {
		return err
	}
	if !item.IsTerminal {
		output, err := s.ddb.Scan(ctx, &dynamodb.ScanInput{
			TableName:                 &s.table,
			ExpressionAttributeNames:  map[string]string{"#key": "Key"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":key": &types.AttributeValueMemberS{Value: key + "/"}},
			FilterExpression:          aws.String("begins_with(#key, :key)"),
		})
		if err != nil {
			return err
		}
		if len(output.Items) == 0 {
			return fs.ErrNotExist
		}
		for _, i := range output.Items {
			if _, err := s.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: &s.table,
				Key: map[string]types.AttributeValue{
					"Key": i["Key"],
				},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Exists returns true if the key exists either as
// a directory (prefix to other keys) or a file,
// and there was no error checking.
func (s *Storage) Exists(ctx context.Context, key string) bool {
	output, err := s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{Value: key},
		},
	})
	return err == nil && len(output.Item) > 0
}

// List returns all keys in the given path.
//
// If recursive is true, non-terminal keys
// will be enumerated (i.e. "directories"
// should be walked); otherwise, only keys
// prefixed exactly by prefix will be listed.
func (s *Storage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	output, err := s.ddb.Scan(ctx, &dynamodb.ScanInput{
		TableName:                 &s.table,
		ExpressionAttributeNames:  map[string]string{"#key": "Key"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":key": &types.AttributeValueMemberS{Value: path + "/"}},
		FilterExpression:          aws.String("begins_with(#key, :key)"),
	})
	if err != nil {
		return nil, err
	}
	if len(output.Items) == 0 {
		return nil, fs.ErrNotExist
	}
	keys := make([]string, 0, len(output.Items))
	for _, i := range output.Items {
		var item Item
		if err := item.Load(i); err != nil {
			return nil, err
		}
		if !recursive {
			// these two paths go through foo:
			// foo/cert/key
			// foo/cert/chain
			//
			// So List(prefix: "foo", recursive: false) would return:
			// foo/cert
			name := strings.TrimPrefix(item.Key, path+"/")
			if strings.Contains(name, "/") {
				continue
			}
		}
		keys = append(keys, item.Key)
	}
	return keys, nil
}

// Stat returns information about key.
func (s *Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	output, err := s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	if len(output.Item) == 0 {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}
	var item Item
	if err := item.Load(output.Item); err != nil {
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        item.Key,
		Modified:   item.Modified,
		IsTerminal: item.IsTerminal,
		Size:       item.Size,
	}, nil
}

// Lock acquires the lock for name, blocking until the lock
// can be obtained or an error is returned. Only one lock
// for the given name can exist at a time. A call to Lock for
// a name which already exists blocks until the named lock
// is released or becomes stale.
//
// If the named lock represents an idempotent operation, callers
// should always check to make sure the work still needs to be
// completed after acquiring the lock. You never know if another
// process already completed the task while you were waiting to
// acquire it.
//
// Implementations should honor context cancellation.
func (s *Storage) Lock(ctx context.Context, name string) error {
	for {
		if _, err := s.ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &s.table,
			Item: map[string]types.AttributeValue{
				"Key":    &types.AttributeValueMemberS{Value: fmt.Sprintf("LOCK-%s", name)},
				"Locked": &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339)},
			},
			ExpressionAttributeNames: map[string]string{"#key": "Key"},
			ConditionExpression:      aws.String("attribute_not_exists(#key)"),
		}); err != nil {
			var condCheckErr *types.ConditionalCheckFailedException
			if errors.As(err, &condCheckErr) {
				if str, ok := condCheckErr.Item["Locked"].(*types.AttributeValueMemberS); ok {
					locked, err := time.Parse(time.RFC3339, str.Value)
					if err != nil {
						return err
					}
					if time.Since(locked) > time.Minute {
						if err := s.deleteLock(ctx, name); err != nil {
							return err
						}
						continue
					}
				} else {
					return errors.New("invalid Locked attribute")
				}
				time.Sleep(time.Second)
				continue
			}
			return err
		} else {
			return nil
		}
	}
}

// Unlock releases named lock. This method must ONLY be called
// after a successful call to Lock, and only after the critical
// section is finished, even if it errored or timed out. Unlock
// cleans up any resources allocated during Lock. Unlock should
// only return an error if the lock was unable to be released.
func (s *Storage) Unlock(ctx context.Context, name string) error {
	return s.deleteLock(ctx, name)
}

func (s *Storage) deleteLock(ctx context.Context, name string) error {
	_, err := s.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.table,
		Key: map[string]types.AttributeValue{
			"Key": &types.AttributeValueMemberS{Value: fmt.Sprintf("LOCK-%s", name)},
		},
	})
	return err
}
