package stocks

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"stocks/internal/domain"
	"stocks/internal/kafka"
	"stocks/internal/usecase"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

//go:generate mkdir -p mock
//go:generate minimock -o ./mock/ -s .go -g
type (
	// SKURepository provides repository methods of SKU service.
	SKURepository interface {
		GetSKUByID(ctx context.Context, skuID domain.SKUID) (domain.SKU, error)
	}

	// StockServiceRepository provides repository methods of stock service.
	StockServiceRepository interface {
		SaveStockItem(ctx context.Context, stockItem domain.StockItem) error
		GetStockItem(ctx context.Context, userID domain.UserID, skuID domain.SKUID) (domain.StockItem, error)
		UpdateStockItem(ctx context.Context, stockItem domain.StockItem) error
		DeleteStockItemFromStorage(ctx context.Context, userID domain.UserID, skuID domain.SKUID) error
		GetStockItemBySku(ct context.Context, skuID domain.SKUID) (domain.StockItem, error)
		ListStockItemsByLocation(ctx context.Context, filter domain.Filter) ([]domain.StockItem, error)
		CountStockItems(ctx context.Context, userID domain.UserID, location string) (uint16, error)
	}
)

type stockServiceUseCase struct {
	SKURepository
	StockServiceRepository
	KafkaProducer kafka.StocksEventProducer
}

var _ usecase.StockServiceUseCase = (*stockServiceUseCase)(nil)

func NewStockServiceUseCase(
	skuRepo SKURepository,
	stockRepo StockServiceRepository,
	kafkaProducer kafka.StocksEventProducer,
) *stockServiceUseCase {
	return &stockServiceUseCase{
		SKURepository:          skuRepo,
		StockServiceRepository: stockRepo,
		KafkaProducer:          kafkaProducer,
	}
}

func (s *stockServiceUseCase) AddStockItem(ctx context.Context, stockItem domain.StockItem) error {
	ctx, span := otel.Tracer(os.Getenv("SERVICE_NAME")).Start(ctx, "StockServiceUseCase.AddStockItem")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_id", fmt.Sprintf("%d", stockItem.UserID)),
		attribute.String("sku_id", fmt.Sprintf("%d", stockItem.Sku.ID)),
		attribute.Int64("count", int64(stockItem.Count)),
		attribute.Int64("price", int64(stockItem.Price)),
		attribute.String("location", stockItem.Location),
	)

	// check sku exist or not.
	sku, err := s.GetSKUByID(ctx, stockItem.Sku.ID)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return err
	}

	stockItem.Sku = sku

	existingStockItem, err := s.GetStockItem(ctx, stockItem.UserID, stockItem.Sku.ID)
	if err != nil {
		if errors.Is(err, domain.ErrStockItemNotFound) {
			err = s.SaveStockItem(ctx, stockItem)
			if err != nil {
				span.SetAttributes(attribute.String("error.message", err.Error()))
				return err
			}

			s.KafkaProducer.ProduceSKUCreated(ctx, kafka.SKUCreatedAndStockChangedPayload{
				SKU:   fmt.Sprintf("%d", stockItem.Sku.ID),
				Count: stockItem.Count,
				Price: stockItem.Price,
			})

			return nil
		}

		span.SetAttributes(attribute.String("error.message", err.Error()))

		return err
	}

	stockItem.Count += existingStockItem.Count

	err = s.UpdateStockItem(ctx, stockItem)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return err
	}

	s.KafkaProducer.ProduceStockChanged(ctx, kafka.SKUCreatedAndStockChangedPayload{
		SKU:   fmt.Sprintf("%d", stockItem.Sku.ID),
		Count: stockItem.Count,
		Price: stockItem.Price,
	})

	return nil
}

func (s *stockServiceUseCase) DeleteStockItem(ctx context.Context, userID domain.UserID, skuID domain.SKUID) error {
	ctx, span := otel.Tracer(os.Getenv("SERVICE_NAME")).Start(ctx, "StockServiceUseCase.DeleteStockItem")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_id", fmt.Sprintf("%d", userID)),
		attribute.String("sku_id", fmt.Sprintf("%d", skuID)),
	)

	err := s.DeleteStockItemFromStorage(ctx, userID, skuID)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return err
	}

	return nil
}

func (s *stockServiceUseCase) GetStockItemBySKU(ctx context.Context, skuID domain.SKUID) (domain.StockItem, error) {
	ctx, span := otel.Tracer(os.Getenv("SERVICE_NAME")).Start(ctx, "StockServiceUseCase.GetStockItemBySKU")
	defer span.End()

	span.SetAttributes(
		attribute.String("sku_id", fmt.Sprintf("%d", skuID)),
	)

	stockItem, err := s.GetStockItemBySku(ctx, skuID)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return domain.StockItem{}, err
	}

	return stockItem, nil
}

func (s *stockServiceUseCase) ListStockItems(ctx context.Context, filter domain.Filter) (domain.PaginatedResponse[domain.StockItem], error) {
	ctx, span := otel.Tracer(os.Getenv("SERVICE_NAME")).Start(ctx, "StockServiceUseCase.ListStockItems")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_id", fmt.Sprintf("%d", filter.UserID)),
		attribute.String("location", filter.Location),
		attribute.Int64("page_size", int64(filter.PageSize)),
		attribute.Int64("current_page", int64(filter.CurrentPage)),
	)

	var paginatedResponse domain.PaginatedResponse[domain.StockItem]
	// count stock items.
	countStockItems, err := s.CountStockItems(ctx, filter.UserID, filter.Location)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return domain.PaginatedResponse[domain.StockItem]{}, err
	}

	paginatedResponse.TotalCount = countStockItems

	listOfStockItems, err := s.ListStockItemsByLocation(ctx, filter)
	if err != nil {
		span.SetAttributes(attribute.String("error.message", err.Error()))
		return domain.PaginatedResponse[domain.StockItem]{}, err
	}

	paginatedResponse.Items = listOfStockItems
	paginatedResponse.PageNumber = int64(math.Ceil(float64(countStockItems) / float64(filter.PageSize)))

	return paginatedResponse, nil
}
