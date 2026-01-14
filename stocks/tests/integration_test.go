package integration

import (
	"context"
	"stocks/internal/domain"
	kafkamock "stocks/internal/kafka/mock"
	"stocks/internal/usecase/stocks"
	stocksmock "stocks/internal/usecase/stocks/mock"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStockServiceIntegration_AddAndGetFlow tests the full add stock item and get flow
func TestStockServiceIntegration_AddAndGetFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	// Setup SKU repository mock
	skuRepoMock.GetSKUByIDMock.
		Expect(minimock.AnyContext, domain.SKUID(1001)).
		Return(domain.SKU{ID: 1001, Name: "T-Shirt", Type: "apparel"}, nil)

	stockItem := domain.StockItem{
		UserID:   1,
		Sku:      domain.SKU{ID: 1001, Name: "T-Shirt", Type: "apparel"},
		Count:    100,
		Price:    50,
		Location: "Ashgabat",
	}

	// First call returns not found (new item)
	stockRepoMock.GetStockItemMock.
		Expect(minimock.AnyContext, domain.UserID(1), domain.SKUID(1001)).
		Return(domain.StockItem{}, domain.ErrStockItemNotFound)

	stockRepoMock.SaveStockItemMock.
		Expect(minimock.AnyContext, stockItem).
		Return(nil)

	kafkaProducerMock.ProduceSKUCreatedMock.Return()

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	// Test add stock item
	err := stockUseCase.AddStockItem(ctx, domain.StockItem{
		UserID:   1,
		Sku:      domain.SKU{ID: 1001},
		Count:    100,
		Price:    50,
		Location: "Ashgabat",
	})
	require.NoError(t, err)

	// Setup for get
	stockRepoMock.GetStockItemBySkuMock.
		Expect(minimock.AnyContext, domain.SKUID(1001)).
		Return(stockItem, nil)

	// Test get stock item
	result, err := stockUseCase.GetStockItemBySKU(ctx, domain.SKUID(1001))
	require.NoError(t, err)
	assert.Equal(t, domain.SKUID(1001), result.Sku.ID)
	assert.Equal(t, uint16(100), result.Count)
}

// TestStockServiceIntegration_UpdateExistingStockFlow tests updating existing stock item
func TestStockServiceIntegration_UpdateExistingStockFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	skuRepoMock.GetSKUByIDMock.
		Expect(minimock.AnyContext, domain.SKUID(2001)).
		Return(domain.SKU{ID: 2001, Name: "Coffee Mug", Type: "accessory"}, nil)

	// Existing stock item has 50 items
	existingItem := domain.StockItem{
		UserID:   2,
		Sku:      domain.SKU{ID: 2001, Name: "Coffee Mug", Type: "accessory"},
		Count:    50,
		Price:    25,
		Location: "Mary",
	}

	stockRepoMock.GetStockItemMock.
		Expect(minimock.AnyContext, domain.UserID(2), domain.SKUID(2001)).
		Return(existingItem, nil)

	// Update should add 30 more (50 + 30 = 80)
	updatedItem := domain.StockItem{
		UserID:   2,
		Sku:      domain.SKU{ID: 2001, Name: "Coffee Mug", Type: "accessory"},
		Count:    80, // 50 + 30
		Price:    30, // New price
		Location: "Mary",
	}

	stockRepoMock.UpdateStockItemMock.
		Expect(minimock.AnyContext, updatedItem).
		Return(nil)

	kafkaProducerMock.ProduceStockChangedMock.Return()

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	// Add 30 more items to existing stock
	err := stockUseCase.AddStockItem(ctx, domain.StockItem{
		UserID:   2,
		Sku:      domain.SKU{ID: 2001},
		Count:    30,
		Price:    30,
		Location: "Mary",
	})
	require.NoError(t, err)
}

// TestStockServiceIntegration_DeleteStockFlow tests the delete stock item flow
func TestStockServiceIntegration_DeleteStockFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	stockRepoMock.DeleteStockItemFromStorageMock.
		Expect(minimock.AnyContext, domain.UserID(3), domain.SKUID(3001)).
		Return(nil)

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	err := stockUseCase.DeleteStockItem(ctx, domain.UserID(3), domain.SKUID(3001))
	require.NoError(t, err)
}

// TestStockServiceIntegration_ListStocksByLocationFlow tests listing stocks by location
func TestStockServiceIntegration_ListStocksByLocationFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	filter := domain.Filter{
		UserID:      4,
		Location:    "Turkmenabat",
		PageSize:    10,
		CurrentPage: 1,
	}

	stockRepoMock.CountStockItemsMock.
		Expect(minimock.AnyContext, domain.UserID(4), "Turkmenabat").
		Return(2, nil)

	stockRepoMock.ListStockItemsByLocationMock.
		Expect(minimock.AnyContext, filter).
		Return([]domain.StockItem{
			{UserID: 4, Sku: domain.SKU{ID: 4001, Name: "Item 1"}, Count: 10, Price: 100, Location: "Turkmenabat"},
			{UserID: 4, Sku: domain.SKU{ID: 4002, Name: "Item 2"}, Count: 20, Price: 50, Location: "Turkmenabat"},
		}, nil)

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	result, err := stockUseCase.ListStockItems(ctx, filter)
	require.NoError(t, err)
	assert.Len(t, result.Items, 2)
	assert.Equal(t, uint16(2), result.TotalCount)
}

// TestStockServiceIntegration_SKUNotFoundFlow tests error handling when SKU doesn't exist
func TestStockServiceIntegration_SKUNotFoundFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	skuRepoMock.GetSKUByIDMock.
		Expect(minimock.AnyContext, domain.SKUID(9999)).
		Return(domain.SKU{}, domain.ErrSKUNotFound)

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	err := stockUseCase.AddStockItem(ctx, domain.StockItem{
		UserID:   5,
		Sku:      domain.SKU{ID: 9999},
		Count:    10,
		Price:    100,
		Location: "Ashgabat",
	})
	assert.Error(t, err)
	assert.Equal(t, domain.ErrSKUNotFound, err)
}

// TestStockServiceIntegration_PaginationFlow tests pagination functionality
func TestStockServiceIntegration_PaginationFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	// Total 25 items, page size 10, should have 3 pages
	filter := domain.Filter{
		UserID:      6,
		Location:    "Balkanabat",
		PageSize:    10,
		CurrentPage: 1,
	}

	stockRepoMock.CountStockItemsMock.
		Expect(minimock.AnyContext, domain.UserID(6), "Balkanabat").
		Return(25, nil)

	// Return first page of 10 items
	items := make([]domain.StockItem, 10)
	for i := 0; i < 10; i++ {
		items[i] = domain.StockItem{
			UserID:   6,
			Sku:      domain.SKU{ID: domain.SKUID(6000 + i), Name: "Item"},
			Count:    uint16(i + 1),
			Price:    100,
			Location: "Balkanabat",
		}
	}

	stockRepoMock.ListStockItemsByLocationMock.
		Expect(minimock.AnyContext, filter).
		Return(items, nil)

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	result, err := stockUseCase.ListStockItems(ctx, filter)
	require.NoError(t, err)
	assert.Len(t, result.Items, 10)
	assert.Equal(t, uint16(25), result.TotalCount)
	assert.Equal(t, int64(3), result.PageNumber) // 25 / 10 = 2.5 -> ceil = 3
}

// TestStockServiceIntegration_MultipleLocationsFlow tests stock management across locations
func TestStockServiceIntegration_MultipleLocationsFlow(t *testing.T) {
	t.Parallel()
	mc := minimock.NewController(t)

	skuRepoMock := stocksmock.NewSKURepositoryMock(mc)
	stockRepoMock := stocksmock.NewStockServiceRepositoryMock(mc)
	kafkaProducerMock := kafkamock.NewStocksEventProducerMock(mc)

	// Setup for Ashgabat location
	filterAshgabat := domain.Filter{
		UserID:      7,
		Location:    "Ashgabat",
		PageSize:    10,
		CurrentPage: 1,
	}

	stockRepoMock.CountStockItemsMock.When(minimock.AnyContext, domain.UserID(7), "Ashgabat").
		Then(3, nil)

	stockRepoMock.ListStockItemsByLocationMock.When(minimock.AnyContext, filterAshgabat).
		Then([]domain.StockItem{
			{UserID: 7, Sku: domain.SKU{ID: 7001}, Count: 10, Location: "Ashgabat"},
			{UserID: 7, Sku: domain.SKU{ID: 7002}, Count: 20, Location: "Ashgabat"},
			{UserID: 7, Sku: domain.SKU{ID: 7003}, Count: 30, Location: "Ashgabat"},
		}, nil)

	// Setup for Mary location
	filterMary := domain.Filter{
		UserID:      7,
		Location:    "Mary",
		PageSize:    10,
		CurrentPage: 1,
	}

	stockRepoMock.CountStockItemsMock.When(minimock.AnyContext, domain.UserID(7), "Mary").
		Then(1, nil)

	stockRepoMock.ListStockItemsByLocationMock.When(minimock.AnyContext, filterMary).
		Then([]domain.StockItem{
			{UserID: 7, Sku: domain.SKU{ID: 7004}, Count: 5, Location: "Mary"},
		}, nil)

	stockUseCase := stocks.NewStockServiceUseCase(skuRepoMock, stockRepoMock, kafkaProducerMock)
	ctx := context.Background()

	// Query Ashgabat
	resultAshgabat, err := stockUseCase.ListStockItems(ctx, filterAshgabat)
	require.NoError(t, err)
	assert.Len(t, resultAshgabat.Items, 3)

	// Query Mary
	resultMary, err := stockUseCase.ListStockItems(ctx, filterMary)
	require.NoError(t, err)
	assert.Len(t, resultMary.Items, 1)
}
