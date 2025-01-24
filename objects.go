// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/zeebo/errs"

	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/testuplink"
)

// ListObjectsOptions defines object listing options.
type ListObjectsOptions struct {
	// Prefix allows to filter objects by a key prefix.
	// If not empty, it must end with slash.
	Prefix string
	// Cursor sets the starting position of the iterator.
	// The first item listed will be the one after the cursor.
	// Cursor is relative to Prefix.
	Cursor string
	// Recursive iterates the objects without collapsing prefixes.
	Recursive bool

	// System includes SystemMetadata in the results.
	System bool
	// Custom includes CustomMetadata in the results.
	Custom bool
}

// ListObjects returns an iterator over the objects.
func (project *Project) ListObjects(ctx context.Context, bucket string, options *ListObjectsOptions) *ObjectIterator {
	defer mon.Task()(&ctx)(nil)

	b := metaclient.Bucket{Name: bucket}
	opts := metaclient.ListOptions{
		Direction: metaclient.After,
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
		opts.IncludeCustomMetadata = options.Custom
		opts.IncludeSystemMetadata = options.System
	}

	opts.Limit = testuplink.GetListLimit(ctx)

	impl := listObjectsIterator{
		ctx:     ctx,
		project: project,
		bucket:  b,
		options: opts,
	}

	objects := ObjectIterator{
		ctx:  ctx,
		impl: &impl,
	}

	if options != nil {
		objects.customMeta = options.Custom
		objects.systemMeta = options.System
		objects.prefix = options.Prefix
	}

	return &objects
}

// ObjectIterator is an iterator over a collection of objects or prefixes.
type ObjectIterator struct {
	ctx        context.Context
	impl       objectIteratorImpl
	customMeta bool
	systemMeta bool
	prefix     string
	list       *metaclient.ObjectList
	position   int
	completed  bool
	err        error
}

type objectIteratorImpl interface {
	loadNext() (list *metaclient.ObjectList, completed bool, err error)
}

// Next prepares next Object for reading.
// It returns false if the end of the iteration is reached and there are no more objects, or if there is an error.
func (objects *ObjectIterator) Next() bool {
	if objects.err != nil {
		objects.completed = true
		return false
	}

	if objects.list == nil {
		objects.list, objects.completed, objects.err = objects.impl.loadNext()
		objects.position = 0
		return !objects.completed
	}

	if objects.position >= len(objects.list.Items)-1 {
		if !objects.list.More {
			objects.completed = true
			return false
		}
		objects.list, objects.completed, objects.err = objects.impl.loadNext()
		objects.position = 0
		return !objects.completed
	}

	objects.position++

	return true
}

// Err returns error, if one happened during iteration.
func (objects *ObjectIterator) Err() error {
	return packageError.Wrap(objects.err)
}

// Item returns the current object in the iterator.
func (objects *ObjectIterator) Item() *Object {
	item := objects.item()
	if item == nil {
		return nil
	}

	key := item.Path
	if len(objects.prefix) > 0 {
		key = objects.prefix + item.Path
	}

	obj := Object{
		Key:      key,
		IsPrefix: item.IsPrefix,
	}

	// TODO: Make this filtering on the satellite
	if objects.systemMeta {
		obj.System = SystemMetadata{
			Created:       item.Created,
			Expires:       item.Expires,
			ContentLength: item.Size,
		}
	}

	// TODO: Make this filtering on the satellite
	if objects.customMeta {
		obj.Custom = item.Metadata
	}

	return &obj
}

func (objects *ObjectIterator) item() *metaclient.Object {
	if objects.completed {
		return nil
	}

	if objects.err != nil {
		return nil
	}

	if objects.list == nil {
		return nil
	}

	if len(objects.list.Items) == 0 {
		return nil
	}

	return &objects.list.Items[objects.position]
}

type listObjectsIterator struct {
	ctx     context.Context
	project *Project
	bucket  metaclient.Bucket
	options metaclient.ListOptions
}

func (objects *listObjectsIterator) loadNext() (list *metaclient.ObjectList, completed bool, err error) {
	db, err := objects.project.dialMetainfoDB(objects.ctx)
	if err != nil {
		return nil, true, convertKnownErrors(err, objects.bucket.Name, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	l, err := db.ListObjects(objects.ctx, objects.bucket.Name, objects.options)
	if err != nil {
		return nil, true, convertKnownErrors(err, objects.bucket.Name, "")
	}
	if l.More {
		objects.options = objects.options.NextPage(l)
	}
	return &l, len(l.Items) == 0, nil
}

// FindObjectsByMetadataOptions defines options for metadata based object queries.
type FindObjectsByMetadataOptions struct {
	Prefix  string
	Limit   int
	Queries []MetadataQuery
}

// MetadataQuery defines a single query for the FindObjecsByMetadata method.
type MetadataQuery interface {
	toQuery() metaclient.MetadataQuery
}

// MetadataQueryMatchValues defines a query that matches metadata values.
type MetadataQueryMatchValues struct {
	Values map[string]interface{}
}

func (query MetadataQueryMatchValues) toQuery() metaclient.MetadataQuery {
	return metaclient.MetadataQueryMatchValues{Values: query.Values}
}

// MetadataQueryJMESPathFilter defines a query that filters metadata using a JMESPath expression.
type MetadataQueryJMESPathFilter struct {
	Expression string
}

func (query MetadataQueryJMESPathFilter) toQuery() metaclient.MetadataQuery {
	return metaclient.MetadataQueryJMESPathFilter{Expression: query.Expression}
}

// MetadataQueryJMESPathProjection defines a query that applies a projection to
// metadata using a JMESPath expression.
type MetadataQueryJMESPathProjection struct {
	Expression string
}

func (query MetadataQueryJMESPathProjection) toQuery() metaclient.MetadataQuery {
	return metaclient.MetadataQueryJMESPathProjection{Expression: query.Expression}
}

func (project *Project) FindObjectsByMetadata(ctx context.Context, bucket string, options *FindObjectsByMetadataOptions) *ObjectIterator {
	defer mon.Task()(&ctx)(nil)

	b := metaclient.Bucket{Name: bucket}
	queries := make([]metaclient.MetadataQuery, len(options.Queries))
	for i, query := range options.Queries {
		queries[i] = query.toQuery()
	}

	impl := findObjectsByMetadataIterator{
		ctx:     ctx,
		project: project,
		bucket:  b,
		options: metaclient.FindObjectsByMetadataOptions{
			Prefix:  options.Prefix,
			Limit:   options.Limit,
			Queries: queries,
		},
	}

	objects := ObjectIterator{
		ctx:        ctx,
		impl:       &impl,
		customMeta: true,
		systemMeta: true,
	}

	return &objects
}

type findObjectsByMetadataIterator struct {
	ctx     context.Context
	project *Project
	bucket  metaclient.Bucket
	options metaclient.FindObjectsByMetadataOptions
}

func (it *findObjectsByMetadataIterator) loadNext() (list *metaclient.ObjectList, completed bool, err error) {
	db, err := it.project.dialMetainfoDB(it.ctx)
	if err != nil {
		return nil, true, convertKnownErrors(err, it.bucket.Name, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	l, err := db.FindObjectsByMetadata(it.ctx, it.bucket.Name, it.options)
	if err != nil {
		return nil, true, convertKnownErrors(err, it.bucket.Name, "")
	}
	if l.More {
		it.options = it.options.NextPage(l)
	}

	return &l, !l.More, nil
}
