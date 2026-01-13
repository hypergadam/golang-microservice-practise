package stocks

import (
	"context"
	"errors"
	"stocks/internal/domain"
	kafkamock "stocks/internal/kafka/mock"
	"stocks/internal/usecase/stocks/mock"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
)

func TestStockServiceUseCase_AddStockItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		stockItem          domain.StockItem
		skuRepoMock        func(mc *minimock.Controller) *mock.SKURepositoryMock
		stockRepoMock      func(mc *minimock.Controller) *mock.StockServiceRepositoryMock
		kafkaProducerMock  func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock
		wantErr            bool
		expectedErr        error
	}{
		{
			name: "new item should be saved",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   1001,
					Name: "t-shirt",
					Type: "apparel",
				},
				Count:    10,
				Price:    12,
				Location: "Ashgabat",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(1001)).
					Return(domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"}, nil)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1001)).
					Return(domain.StockItem{}, domain.ErrStockItemNotFound)
				ssrm.SaveStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
						Count:    10,
						Price:    12,
						Location: "Ashgabat",
					}).
					Return(nil)
				return ssrm
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				kpm := kafkamock.NewStocksEventProducerMock(mc)
				kpm.ProduceSKUCreatedMock.Return()
				return kpm
			},
			wantErr: false,
		},
		{
			name: "existing stock item should be updated",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   2020,
					Name: "cup",
					Type: "accessory",
				},
				Count:    5,
				Price:    20,
				Location: "Ashgabat",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(2020)).
					Return(domain.SKU{ID: 2020, Name: "cup", Type: "accessory"}, nil)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(2020)).
					Return(domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 2020, Name: "cup", Type: "accessory"},
						Count:    10,
						Price:    12,
						Location: "Ashgabat",
					}, nil)
				ssrm.UpdateStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 2020, Name: "cup", Type: "accessory"},
						Count:    15, // 10 + 5
						Price:    20,
						Location: "Ashgabat",
					}).Return(nil)
				return ssrm
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				kpm := kafkamock.NewStocksEventProducerMock(mc)
				kpm.ProduceStockChangedMock.Return()
				return kpm
			},
			wantErr: false,
		},
		{
			name: "sku not found should return error",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   1002,
					Name: "t-shirt",
					Type: "apparel",
				},
				Count:    8,
				Price:    30,
				Location: "Ashgabat",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(1002)).
					Return(domain.SKU{}, domain.ErrSKUNotFound)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				// No calls expected
				return mock.NewStockServiceRepositoryMock(mc)
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				// No calls expected
				return kafkamock.NewStocksEventProducerMock(mc)
			},
			wantErr:     true,
			expectedErr: domain.ErrSKUNotFound,
		},
		{
			name: "get stock item error except not found",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   1003,
					Name: "t-shirt",
					Type: "apparel",
				},
				Count:    7,
				Price:    25,
				Location: "Ashgabat",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(1003)).
					Return(domain.SKU{ID: 1003, Name: "t-shirt", Type: "apparel"}, nil)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1003)).
					Return(domain.StockItem{}, errors.New("database error"))
				return ssrm
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				// No calls expected
				return kafkamock.NewStocksEventProducerMock(mc)
			},
			wantErr:     true,
			expectedErr: errors.New("database error"),
		},
		{
			name: "save stock item error",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   1002,
					Name: "jacket",
					Type: "apparel",
				},
				Count:    7,
				Price:    25,
				Location: "Ashgabat",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(1002)).
					Return(domain.SKU{ID: 1002, Name: "jacket", Type: "apparel"}, nil)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1002)).
					Return(domain.StockItem{}, domain.ErrStockItemNotFound)
				ssrm.SaveStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 1002, Name: "jacket", Type: "apparel"},
						Count:    7,
						Price:    25,
						Location: "Ashgabat",
					}).
					Return(errors.New("save stock item failed"))
				return ssrm
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				// No calls expected (save failed before kafka produce)
				return kafkamock.NewStocksEventProducerMock(mc)
			},
			wantErr:     true,
			expectedErr: errors.New("save stock item failed"),
		},
		{
			name: "update stock item error",
			stockItem: domain.StockItem{
				UserID: 1,
				Sku: domain.SKU{
					ID:   3030,
					Name: "pen",
					Type: "stationery",
				},
				Count:    3,
				Price:    5,
				Location: "Mary",
			},
			skuRepoMock: func(mc *minimock.Controller) *mock.SKURepositoryMock {
				sm := mock.NewSKURepositoryMock(mc)
				sm.GetSKUByIDMock.
					Expect(minimock.AnyContext, domain.SKUID(3030)).
					Return(domain.SKU{ID: 3030, Name: "pen", Type: "stationery"}, nil)
				return sm
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(3030)).
					Return(domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 3030, Name: "pen", Type: "stationery"},
						Count:    2,
						Price:    5,
						Location: "Mary",
					}, nil)
				ssrm.UpdateStockItemMock.
					Expect(minimock.AnyContext, domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 3030, Name: "pen", Type: "stationery"},
						Count:    5, // 2 + 3
						Price:    5,
						Location: "Mary",
					}).Return(errors.New("update stock item failed"))
				return ssrm
			},
			kafkaProducerMock: func(mc *minimock.Controller) *kafkamock.StocksEventProducerMock {
				// No calls expected (update failed before kafka produce)
				return kafkamock.NewStocksEventProducerMock(mc)
			},
			wantErr:     true,
			expectedErr: errors.New("update stock item failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			skuRepo := tt.skuRepoMock(mc)
			stockRepo := tt.stockRepoMock(mc)
			kafkaProducer := tt.kafkaProducerMock(mc)

			newStockUseCase := NewStockServiceUseCase(skuRepo, stockRepo, kafkaProducer)
			err := newStockUseCase.AddStockItem(context.Background(), tt.stockItem)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.Equal(t, tt.expectedErr.Error(), err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStockServiceUseCase_DeleteStockItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		userID        domain.UserID
		skuID         domain.SKUID
		stockRepoMock func(mc *minimock.Controller) *mock.StockServiceRepositoryMock
		wantErr       bool
		expectedErr   error
	}{
		{
			name:   "successful delete",
			userID: 1,
			skuID:  1001,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.DeleteStockItemFromStorageMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1001)).
					Return(nil)
				return ssrm
			},
			wantErr: false,
		},
		{
			name:   "delete stock item not found",
			userID: 1,
			skuID:  9999,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.DeleteStockItemFromStorageMock.
					Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(9999)).
					Return(domain.ErrStockItemNotFound)
				return ssrm
			},
			wantErr:     true,
			expectedErr: domain.ErrStockItemNotFound,
		},
		{
			name:   "delete stock item database error",
			userID: 2,
			skuID:  2002,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.DeleteStockItemFromStorageMock.
					Expect(minimock.AnyContext, domain.UserID(2), domain.SKUID(2002)).
					Return(errors.New("database connection error"))
				return ssrm
			},
			wantErr:     true,
			expectedErr: errors.New("database connection error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			// SKU repo and Kafka not needed for delete
			skuRepo := mock.NewSKURepositoryMock(mc)
			stockRepo := tt.stockRepoMock(mc)
			kafkaProducer := kafkamock.NewStocksEventProducerMock(mc)

			newStockUseCase := NewStockServiceUseCase(skuRepo, stockRepo, kafkaProducer)
			err := newStockUseCase.DeleteStockItem(context.Background(), tt.userID, tt.skuID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.Equal(t, tt.expectedErr.Error(), err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStockServiceUseCase_GetStockItemBySKU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		skuID         domain.SKUID
		stockRepoMock func(mc *minimock.Controller) *mock.StockServiceRepositoryMock
		wantStockItem domain.StockItem
		wantErr       bool
		expectedErr   error
	}{
		{
			name:  "successful get stock item by sku",
			skuID: 1001,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemBySkuMock.
					Expect(minimock.AnyContext, domain.SKUID(1001)).
					Return(domain.StockItem{
						UserID:   1,
						Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
						Count:    10,
						Price:    12,
						Location: "Ashgabat",
					}, nil)
				return ssrm
			},
			wantStockItem: domain.StockItem{
				UserID:   1,
				Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
				Count:    10,
				Price:    12,
				Location: "Ashgabat",
			},
			wantErr: false,
		},
		{
			name:  "stock item not found by sku",
			skuID: 9999,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemBySkuMock.
					Expect(minimock.AnyContext, domain.SKUID(9999)).
					Return(domain.StockItem{}, domain.ErrStockItemNotFound)
				return ssrm
			},
			wantStockItem: domain.StockItem{},
			wantErr:       true,
			expectedErr:   domain.ErrStockItemNotFound,
		},
		{
			name:  "database error when getting stock item by sku",
			skuID: 2002,
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.GetStockItemBySkuMock.
					Expect(minimock.AnyContext, domain.SKUID(2002)).
					Return(domain.StockItem{}, errors.New("database error"))
				return ssrm
			},
			wantStockItem: domain.StockItem{},
			wantErr:       true,
			expectedErr:   errors.New("database error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			skuRepo := mock.NewSKURepositoryMock(mc)
			stockRepo := tt.stockRepoMock(mc)
			kafkaProducer := kafkamock.NewStocksEventProducerMock(mc)

			newStockUseCase := NewStockServiceUseCase(skuRepo, stockRepo, kafkaProducer)
			stockItem, err := newStockUseCase.GetStockItemBySKU(context.Background(), tt.skuID)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.Equal(t, tt.expectedErr.Error(), err.Error())
				}
				assert.Equal(t, domain.StockItem{}, stockItem)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantStockItem, stockItem)
			}
		})
	}
}

func TestStockServiceUseCase_ListStockItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filter        domain.Filter
		stockRepoMock func(mc *minimock.Controller) *mock.StockServiceRepositoryMock
		wantResponse  domain.PaginatedResponse[domain.StockItem]
		wantErr       bool
		expectedErr   error
	}{
		{
			name: "successful list stock items",
			filter: domain.Filter{
				UserID:      1,
				Location:    "Ashgabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.CountStockItemsMock.
					Expect(minimock.AnyContext, domain.UserID(1), "Ashgabat").
					Return(2, nil)
				ssrm.ListStockItemsByLocationMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      1,
						Location:    "Ashgabat",
						PageSize:    10,
						CurrentPage: 1,
					}).
					Return([]domain.StockItem{
						{
							UserID:   1,
							Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
							Count:    10,
							Price:    12,
							Location: "Ashgabat",
						},
						{
							UserID:   1,
							Sku:      domain.SKU{ID: 1002, Name: "cup", Type: "accessory"},
							Count:    5,
							Price:    20,
							Location: "Ashgabat",
						},
					}, nil)
				return ssrm
			},
			wantResponse: domain.PaginatedResponse[domain.StockItem]{
				Items: []domain.StockItem{
					{
						UserID:   1,
						Sku:      domain.SKU{ID: 1001, Name: "t-shirt", Type: "apparel"},
						Count:    10,
						Price:    12,
						Location: "Ashgabat",
					},
					{
						UserID:   1,
						Sku:      domain.SKU{ID: 1002, Name: "cup", Type: "accessory"},
						Count:    5,
						Price:    20,
						Location: "Ashgabat",
					},
				},
				TotalCount: 2,
				PageNumber: 1,
			},
			wantErr: false,
		},
		{
			name: "count stock items error",
			filter: domain.Filter{
				UserID:      1,
				Location:    "Ashgabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.CountStockItemsMock.
					Expect(minimock.AnyContext, domain.UserID(1), "Ashgabat").
					Return(0, errors.New("count error"))
				return ssrm
			},
			wantResponse: domain.PaginatedResponse[domain.StockItem]{},
			wantErr:      true,
			expectedErr:  errors.New("count error"),
		},
		{
			name: "list stock items error",
			filter: domain.Filter{
				UserID:      2,
				Location:    "Mary",
				PageSize:    5,
				CurrentPage: 1,
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.CountStockItemsMock.
					Expect(minimock.AnyContext, domain.UserID(2), "Mary").
					Return(5, nil)
				ssrm.ListStockItemsByLocationMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      2,
						Location:    "Mary",
						PageSize:    5,
						CurrentPage: 1,
					}).
					Return(nil, errors.New("list error"))
				return ssrm
			},
			wantResponse: domain.PaginatedResponse[domain.StockItem]{},
			wantErr:      true,
			expectedErr:  errors.New("list error"),
		},
		{
			name: "empty list of stock items",
			filter: domain.Filter{
				UserID:      3,
				Location:    "Balkanabat",
				PageSize:    10,
				CurrentPage: 1,
			},
			stockRepoMock: func(mc *minimock.Controller) *mock.StockServiceRepositoryMock {
				ssrm := mock.NewStockServiceRepositoryMock(mc)
				ssrm.CountStockItemsMock.
					Expect(minimock.AnyContext, domain.UserID(3), "Balkanabat").
					Return(0, nil)
				ssrm.ListStockItemsByLocationMock.
					Expect(minimock.AnyContext, domain.Filter{
						UserID:      3,
						Location:    "Balkanabat",
						PageSize:    10,
						CurrentPage: 1,
					}).
					Return([]domain.StockItem{}, nil)
				return ssrm
			},
			wantResponse: domain.PaginatedResponse[domain.StockItem]{
				Items:      []domain.StockItem{},
				TotalCount: 0,
				PageNumber: 0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc := minimock.NewController(t)

			skuRepo := mock.NewSKURepositoryMock(mc)
			stockRepo := tt.stockRepoMock(mc)
			kafkaProducer := kafkamock.NewStocksEventProducerMock(mc)

			newStockUseCase := NewStockServiceUseCase(skuRepo, stockRepo, kafkaProducer)
			response, err := newStockUseCase.ListStockItems(context.Background(), tt.filter)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.Equal(t, tt.expectedErr.Error(), err.Error())
				}
				assert.Equal(t, domain.PaginatedResponse[domain.StockItem]{}, response)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantResponse, response)
			}
		})
	}
}
