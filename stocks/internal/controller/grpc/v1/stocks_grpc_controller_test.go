package v1

import (
	"context"
	"errors"
	"stocks/internal/domain"
	usecasemock "stocks/internal/usecase/mock"
	pb "stocks/pkg/api/stocks"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStockGRPCHandler_AddStockItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *pb.CreateStockItemRequest
		useCaseMock func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock
		wantCode    codes.Code
		wantSuccess bool
	}{
		{
			name: "successful add stock item",
			req: &pb.CreateStockItemRequest{
				UserId:   1,
				SkuId:    1001,
				Count:    10,
				Price:    100,
				Location: "Ashgabat",
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.AddStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID: 1,
						Sku:    domain.SKU{ID: 1001},
						Count:  10,
						Price:  100,
						Location: "Ashgabat",
					}).
					Return(nil)
				return ucm
			},
			wantCode:    codes.OK,
			wantSuccess: true,
		},
		{
			name: "SKU not found",
			req: &pb.CreateStockItemRequest{
				UserId:   1,
				SkuId:    9999,
				Count:    5,
				Price:    50,
				Location: "Mary",
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.AddStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID: 1,
						Sku:    domain.SKU{ID: 9999},
						Count:  5,
						Price:  50,
						Location: "Mary",
					}).
					Return(domain.ErrSKUNotFound)
				return ucm
			},
			wantCode:    codes.NotFound,
			wantSuccess: false,
		},
		{
			name: "invalid request - zero user ID",
			req: &pb.CreateStockItemRequest{
				UserId:   0,
				SkuId:    1001,
				Count:    10,
				Price:    100,
				Location: "Ashgabat",
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				return usecasemock.NewStockServiceUseCaseMock(mc)
			},
			wantCode:    codes.InvalidArgument,
			wantSuccess: false,
		},
		{
			name: "internal error",
			req: &pb.CreateStockItemRequest{
				UserId:   1,
				SkuId:    1002,
				Count:    5,
				Price:    50,
				Location: "Turkmenabat",
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.AddStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID: 1,
						Sku:    domain.SKU{ID: 1002},
						Count:  5,
						Price:  50,
						Location: "Turkmenabat",
					}).
					Return(errors.New("database error"))
				return ucm
			},
			wantCode:    codes.Internal,
			wantSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			ucMock := tt.useCaseMock(mc)
			handler := NewStockGRPCHandler(ucMock)

			resp, err := handler.AddStockItem(context.Background(), tt.req)

			if tt.wantCode == codes.OK {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.wantSuccess, resp.Success)
			} else {
				assert.Error(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
			}
		})
	}
}

func TestStockGRPCHandler_DeleteStockItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *pb.DeleteStockItemRequest
		useCaseMock func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock
		wantCode    codes.Code
		wantSuccess bool
	}{
		{
			name: "successful delete",
			req: &pb.DeleteStockItemRequest{
				UserId: 1,
				SkuId:  1001,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.DeleteStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1001)).
					Return(nil)
				return ucm
			},
			wantCode:    codes.OK,
			wantSuccess: true,
		},
		{
			name: "stock item not found",
			req: &pb.DeleteStockItemRequest{
				UserId: 1,
				SkuId:  9999,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.DeleteStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(9999)).
					Return(domain.ErrStockItemNotFound)
				return ucm
			},
			wantCode:    codes.NotFound,
			wantSuccess: false,
		},
		{
			name: "invalid request - zero user ID",
			req: &pb.DeleteStockItemRequest{
				UserId: 0,
				SkuId:  1001,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				return usecasemock.NewStockServiceUseCaseMock(mc)
			},
			wantCode:    codes.InvalidArgument,
			wantSuccess: false,
		},
		{
			name: "internal error",
			req: &pb.DeleteStockItemRequest{
				UserId: 1,
				SkuId:  2002,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.DeleteStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(2002)).
					Return(errors.New("database error"))
				return ucm
			},
			wantCode:    codes.Internal,
			wantSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			ucMock := tt.useCaseMock(mc)
			handler := NewStockGRPCHandler(ucMock)

			resp, err := handler.DeleteStockItem(context.Background(), tt.req)

			if tt.wantCode == codes.OK {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.wantSuccess, resp.Success)
			} else {
				assert.Error(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
			}
		})
	}
}

func TestStockGRPCHandler_GetStockItemBySKU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          *pb.GetStockItemRequest
		useCaseMock  func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock
		wantCode     codes.Code
		wantSkuID    uint32
		wantName     string
	}{
		{
			name: "successful get stock item",
			req: &pb.GetStockItemRequest{
				SkuId: 1001,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.GetStockItemBySKUMock.
					Expect(minimock.AnyContext, domain.SKUID(1001)).
					Return(domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
						Count:    10,
						Price:    100,
						Location: "Ashgabat",
					}, nil)
				return ucm
			},
			wantCode:  codes.OK,
			wantSkuID: 1001,
			wantName:  "t-shirt",
		},
		{
			name: "stock item not found",
			req: &pb.GetStockItemRequest{
				SkuId: 9999,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.GetStockItemBySKUMock.
					Expect(minimock.AnyContext, domain.SKUID(9999)).
					Return(domain.StockItem{}, domain.ErrStockItemNotFound)
				return ucm
			},
			wantCode: codes.NotFound,
		},
		{
			name: "invalid request - zero SKU ID",
			req: &pb.GetStockItemRequest{
				SkuId: 0,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				return usecasemock.NewStockServiceUseCaseMock(mc)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "internal error",
			req: &pb.GetStockItemRequest{
				SkuId: 2002,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.GetStockItemBySKUMock.
					Expect(minimock.AnyContext, domain.SKUID(2002)).
					Return(domain.StockItem{}, errors.New("database error"))
				return ucm
			},
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			ucMock := tt.useCaseMock(mc)
			handler := NewStockGRPCHandler(ucMock)

			resp, err := handler.GetStockItemBySKU(context.Background(), tt.req)

			if tt.wantCode == codes.OK {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.wantSkuID, resp.SkuId)
				assert.Equal(t, tt.wantName, resp.Name)
			} else {
				assert.Error(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
			}
		})
	}
}

func TestStockGRPCHandler_ListStockItemsByLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		req            *pb.FilterRequest
		useCaseMock    func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock
		wantCode       codes.Code
		wantItemsCount int
		wantTotalCount uint32
	}{
		{
			name: "successful list stock items",
			req: &pb.FilterRequest{
				UserId:      1,
				Location:    "Ashgabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.ListStockItemsMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      1,
						Location:    "Ashgabat",
						PageSize:    10,
						CurrentPage: 1,
					}).
					Return(domain.PaginatedResponse[domain.StockItem]{
						Items: []domain.StockItem{
							{UserID: 1, Sku: domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"}, Count: 10, Price: 100, Location: "Ashgabat"},
							{UserID: 1, Sku: domain.SKU{ID: 1002, Name: "cup", Type: "accessory"}, Count: 5, Price: 50, Location: "Ashgabat"},
						},
						TotalCount: 2,
						PageNumber: 1,
					}, nil)
				return ucm
			},
			wantCode:       codes.OK,
			wantItemsCount: 2,
			wantTotalCount: 2,
		},
		{
			name: "empty list",
			req: &pb.FilterRequest{
				UserId:      2,
				Location:    "Mary",
				PageSize:    10,
				CurrentPage: 1,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.ListStockItemsMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      2,
						Location:    "Mary",
						PageSize:    10,
						CurrentPage: 1,
					}).
					Return(domain.PaginatedResponse[domain.StockItem]{
						Items:      []domain.StockItem{},
						TotalCount: 0,
						PageNumber: 0,
					}, nil)
				return ucm
			},
			wantCode:       codes.OK,
			wantItemsCount: 0,
			wantTotalCount: 0,
		},
		{
			name: "invalid request - zero user ID",
			req: &pb.FilterRequest{
				UserId:      0,
				Location:    "Ashgabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				return usecasemock.NewStockServiceUseCaseMock(mc)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "internal error",
			req: &pb.FilterRequest{
				UserId:      3,
				Location:    "Turkmenabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			useCaseMock: func(mc *minimock.Controller) *usecasemock.StockServiceUseCaseMock {
				ucm := usecasemock.NewStockServiceUseCaseMock(mc)
				ucm.ListStockItemsMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      3,
						Location:    "Turkmenabat",
						PageSize:    10,
						CurrentPage: 1,
					}).
					Return(domain.PaginatedResponse[domain.StockItem]{}, errors.New("database error"))
				return ucm
			},
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			ucMock := tt.useCaseMock(mc)
			handler := NewStockGRPCHandler(ucMock)

			resp, err := handler.ListStockItemsByLocation(context.Background(), tt.req)

			if tt.wantCode == codes.OK {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tt.wantItemsCount, len(resp.Items))
				assert.Equal(t, tt.wantTotalCount, resp.TotalCount)
			} else {
				assert.Error(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
			}
		})
	}
}
