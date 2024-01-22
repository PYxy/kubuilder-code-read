/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

const defaultPageSize = 500
const defaultPageBufferSize = 10

// ListPageFunc returns a list object for the given list options.
type ListPageFunc func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error)

// SimplePageFunc adapts a context-less list function into one that accepts a context.
func SimplePageFunc(fn func(opts metav1.ListOptions) (runtime.Object, error)) ListPageFunc {
	return func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		return fn(opts)
	}
}

// ListPager assists client code in breaking large list queries into multiple
// smaller chunks of PageSize or smaller. PageFn is expected to accept a
// metav1.ListOptions that supports paging and return a list. The pager does
// not alter the field or label selectors on the initial options list.
type ListPager struct {
	PageSize int64
	PageFn   ListPageFunc

	FullListIfExpired bool

	// Number of pages to buffer
	PageBufferSize int32
}

// New creates a new pager from the provided pager function using the default
// options. It will fall back to a full list if an expiration error is encountered
// as a last resort.
func New(fn ListPageFunc) *ListPager {
	return &ListPager{
		PageSize:          defaultPageSize,
		PageFn:            fn,
		FullListIfExpired: true,
		PageBufferSize:    defaultPageBufferSize,
	}
}

// TODO: introduce other types of paging functions - such as those that retrieve from a list
// of namespaces.

// List returns a single list object, but attempts to retrieve smaller chunks from the
// server to reduce the impact on the server. If the chunk attempt fails, it will load
// the full list instead. The Limit field on options, if unset, will default to the page size.
func (p *ListPager) List(ctx context.Context, options metav1.ListOptions) (runtime.Object, bool, error) {
	// 设置 page size
	if options.Limit == 0 {
		options.Limit = p.PageSize
	}
	requestedResourceVersion := options.ResourceVersion
	requestedResourceVersionMatch := options.ResourceVersionMatch
	var list *metainternalversion.List // 用来存储获取的结果
	paginatedResult := false

	for {
		select {
		case <-ctx.Done():
			return nil, paginatedResult, ctx.Err()
		default:
		}
		// 真正执行外部传入的，含有list对象的 list 方法 ,就是一个 client 发一个请求给api-server 一个get 请求
		//k8s.io/client-go/tools/cache/listwatch.go
		//NewFilteredListWatchFromClient
		obj, err := p.PageFn(ctx, options)
		if err != nil {
			//失败了 再做一次
			// Only fallback to full list if an "Expired" errors is returned, FullListIfExpired is true, and
			// the "Expired" error occurred in page 2 or later (since full list is intended to prevent a pager.List from
			// failing when the resource versions is established by the first page request falls out of the compaction
			// during the subsequent list requests).
			if !errors.IsResourceExpired(err) || !p.FullListIfExpired || options.Continue == "" {
				return nil, paginatedResult, err
			}
			// the list expired while we were processing, fall back to a full list at
			// the requested ResourceVersion.
			options.Limit = 0
			options.Continue = ""
			options.ResourceVersion = requestedResourceVersion
			options.ResourceVersionMatch = requestedResourceVersionMatch
			result, err := p.PageFn(ctx, options)
			//失败之后,在执行一次 无论成功与否  都直接返回
			return result, paginatedResult, err
		}
		m, err := meta.ListAccessor(obj)
		if err != nil {
			return nil, paginatedResult, fmt.Errorf("returned object must be a list: %v", err)
		}

		// exit early and return the object we got if we haven't processed any pages
		// 判断结果状态，如果没有更多或者已经为空，则退出
		if len(m.GetContinue()) == 0 && list == nil {
			return obj, paginatedResult, nil
		}

		// initialize the list and fill its contents
		if list == nil {
			// 进到这里，表示是第一次执行
			// 使用创建的提取器初始化 list 对象
			list = &metainternalversion.List{Items: make([]runtime.Object, 0, options.Limit+1)}
			list.ResourceVersion = m.GetResourceVersion()
			list.SelfLink = m.GetSelfLink()
		}
		// 处理每一个获取的对象，无论是一个列表，还是封装在一个列表对象中的列表
		if err := meta.EachListItem(obj, func(obj runtime.Object) error {
			list.Items = append(list.Items, obj)
			return nil
		}); err != nil {
			return nil, paginatedResult, err
		}

		// 如果获取完成，就返回
		// if we have no more items, return the list
		if len(m.GetContinue()) == 0 {
			return list, paginatedResult, nil
		}
		// 开始下一次循环的初始化设置
		// set the next loop up
		options.Continue = m.GetContinue()
		// Clear the ResourceVersion(Match) on the subsequent List calls to avoid the
		// `specifying resource version is not allowed when using continue` error.
		// See https://github.com/kubernetes/kubernetes/issues/85221#issuecomment-553748143.
		// 一些变量的清理工作
		options.ResourceVersion = ""
		options.ResourceVersionMatch = ""
		// At this point, result is already paginated.
		// 如果走到这里 说明就肯定是分页了
		paginatedResult = true
	}
}

// EachListItem fetches runtime.Object items using this ListPager and invokes fn on each item. If
// fn returns an error, processing stops and that error is returned. If fn does not return an error,
// any error encountered while retrieving the list from the server is returned. If the context
// cancels or times out, the context error is returned. Since the list is retrieved in paginated
// chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if the pagination list
// requests exceed the expiration limit of the apiserver being called.
//
// Items are retrieved in chunks from the server to reduce the impact on the server with up to
// ListPager.PageBufferSize chunks buffered concurrently in the background.
func (p *ListPager) EachListItem(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	return p.eachListChunkBuffered(ctx, options, func(obj runtime.Object) error {
		return meta.EachListItem(obj, fn)
	})
}

// eachListChunkBuffered fetches runtimeObject list chunks using this ListPager and invokes fn on
// each list chunk.  If fn returns an error, processing stops and that error is returned. If fn does
// not return an error, any error encountered while retrieving the list from the server is
// returned. If the context cancels or times out, the context error is returned. Since the list is
// retrieved in paginated chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if
// the pagination list requests exceed the expiration limit of the apiserver being called.
//
// Up to ListPager.PageBufferSize chunks are buffered concurrently in the background.
func (p *ListPager) eachListChunkBuffered(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	if p.PageBufferSize < 0 {
		return fmt.Errorf("ListPager.PageBufferSize must be >= 0, got %d", p.PageBufferSize)
	}

	// Ensure background goroutine is stopped if this call exits before all list items are
	// processed. Cancelation error from this deferred cancel call is never returned to caller;
	// either the list result has already been sent to bgResultC or the fn error is returned and
	// the cancelation error is discarded.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chunkC := make(chan runtime.Object, p.PageBufferSize)
	bgResultC := make(chan error, 1)
	go func() {
		defer utilruntime.HandleCrash()

		var err error
		defer func() {
			close(chunkC)
			bgResultC <- err
		}()
		err = p.eachListChunk(ctx, options, func(chunk runtime.Object) error {
			select {
			case chunkC <- chunk: // buffer the chunk, this can block
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	for o := range chunkC {
		err := fn(o)
		if err != nil {
			return err // any fn error should be returned immediately
		}
	}
	// promote the results of our background goroutine to the foreground
	return <-bgResultC
}

// eachListChunk fetches runtimeObject list chunks using this ListPager and invokes fn on each list
// chunk. If fn returns an error, processing stops and that error is returned. If fn does not return
// an error, any error encountered while retrieving the list from the server is returned. If the
// context cancels or times out, the context error is returned. Since the list is retrieved in
// paginated chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if the
// pagination list requests exceed the expiration limit of the apiserver being called.
func (p *ListPager) eachListChunk(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	if options.Limit == 0 {
		options.Limit = p.PageSize
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := p.PageFn(ctx, options)
		if err != nil {
			return err
		}
		m, err := meta.ListAccessor(obj)
		if err != nil {
			return fmt.Errorf("returned object must be a list: %v", err)
		}
		if err := fn(obj); err != nil {
			return err
		}
		// if we have no more items, return.
		if len(m.GetContinue()) == 0 {
			return nil
		}
		// set the next loop up
		options.Continue = m.GetContinue()
	}
}
