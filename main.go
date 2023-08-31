package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type Key struct {
	Organization    string
	EnterpriseUnit  string
	SettlementDocID string // Represents the settlement document business period this key is used for
}

func (k Key) BaseKey() string {
	return fmt.Sprintf("org:%s:eu:%s:settlement-id:%s", k.Organization, k.EnterpriseUnit, k.SettlementDocID)
}

func (k Key) TillsSetKey() string {
	return fmt.Sprintf("%s:tills", k.BaseKey())
}

func (k Key) TendersSetKey(till string) string {
	return fmt.Sprintf("%s:till:%s:tenders", k.BaseKey(), till)
}

func (k Key) TenderKey(till, tender string) string {
	return fmt.Sprintf("%s:till:%s:tender:%s", k.BaseKey(), till, tender)
}

func (k Key) DenominationsSetKey(till, tender string) string {
	return fmt.Sprintf("%s:till:%s:tender:%s:denominations", k.BaseKey(), till, tender)
}

func (k Key) DenominationKey(till, tender, denomination string) string {
	return fmt.Sprintf("%s:till:%s:tender:%s:denomination:%s", k.BaseKey(), till, tender, denomination)
}

type Client struct {
	*redis.Client
}

type TenderInfo struct {
	Name   string // Denoination name
	Count  int
	Amount float64
}

type Tender struct {
	ID               string
	Amount           float64
	TenderBreakdowns []TenderInfo
}

type Till struct {
	ID      string
	Tenders []Tender
}

func (c Client) GetExpectedTenders(ctx context.Context, key Key) ([]Till, error) {
	var tills []Till
	tillIDs := c.SMembers(ctx, key.TillsSetKey()).Val()
	for _, tillID := range tillIDs {
		tenderIDs := c.SMembers(ctx, key.TendersSetKey(tillID)).Val()
		var tenders []Tender
		for _, tenderID := range tenderIDs {
			denominationNames := c.SMembers(ctx, key.DenominationsSetKey(tillID, tenderID)).Val()
			var denominations []TenderInfo
			for _, denominationName := range denominationNames {
				denomination := c.HGetAll(ctx, key.DenominationKey(tillID, tenderID, denominationName)).Val()
				count, err := strconv.ParseInt(denomination["count"], 0, 0)
				if err != nil {
					return nil, err
				}
				amount, err := strconv.ParseFloat(denomination["amount"], 64)
				if err != nil {
					return nil, err
				}
				denominations = append(denominations, TenderInfo{
					Name:   denominationName,
					Count:  int(count),
					Amount: amount,
				})
			}

			tenderAmount, err := strconv.ParseFloat(c.Get(ctx, key.TenderKey(tillID, tenderID)).Val(), 64)
			if err != nil {
				return nil, err
			}
			tenders = append(tenders, Tender{
				ID:               tenderID,
				Amount:           tenderAmount,
				TenderBreakdowns: denominations,
			})
		}
		tills = append(tills, Till{
			ID:      tillID,
			Tenders: tenders,
		})
	}

	return tills, nil
}

type Transaction struct {
	Org             string
	EU              string
	SettlementDocID string
	Source          string
	Destination     string
	Direction       string
	Tenders         []Tender
}

func (c Client) ProcessTransaction(t Transaction) error {
	key := Key{
		Organization:    t.Org,
		EnterpriseUnit:  t.EU,
		SettlementDocID: t.SettlementDocID,
	}
	ctx := context.Background()

	direction := 1
	switch t.Direction {
	case ">":
		direction = 1
	case "<":
		direction = -1
	default:
		return fmt.Errorf("invalid direction %s", t.Direction)
	}

	var tenderIDs []interface{}
	for _, tender := range t.Tenders {
		var denominationNames []interface{}
		for _, denomination := range tender.TenderBreakdowns {
			denominationNames = append(denominationNames, denomination.Name)
			if err := c.HIncrByFloat(ctx, key.DenominationKey(t.Destination, tender.ID, denomination.Name), "amount", float64(direction)*denomination.Amount).Err(); err != nil {
				return err
			}
			if err := c.HIncrBy(ctx, key.DenominationKey(t.Destination, tender.ID, denomination.Name), "count", int64(direction*denomination.Count)).Err(); err != nil {
				return err
			}
			if err := c.HIncrByFloat(ctx, key.DenominationKey(t.Source, tender.ID, denomination.Name), "amount", float64(direction)*-denomination.Amount).Err(); err != nil {
				return err
			}
			if err := c.HIncrBy(ctx, key.DenominationKey(t.Source, tender.ID, denomination.Name), "count", int64(direction*-denomination.Count)).Err(); err != nil {
				return err
			}

		}
		if len(denominationNames) > 0 {
			if err := c.SAdd(ctx, key.DenominationsSetKey(t.Source, tender.ID), denominationNames...).Err(); err != nil {
				return err
			}
			if err := c.SAdd(ctx, key.DenominationsSetKey(t.Destination, tender.ID), denominationNames...).Err(); err != nil {
				return err
			}
		}

		// Increment dest tender
		if err := c.IncrByFloat(ctx, key.TenderKey(t.Destination, tender.ID), float64(direction)*tender.Amount).Err(); err != nil {
			return err
		}
		// Decrement source tender
		if err := c.IncrByFloat(ctx, key.TenderKey(t.Source, tender.ID), float64(direction)*-tender.Amount).Err(); err != nil {
			return err
		}

		tenderIDs = append(tenderIDs, tender.ID)
	}

	if len(tenderIDs) > 0 {
		// Add tenders to tenders set for both source and dest
		if err := c.SAdd(ctx, key.TendersSetKey(t.Source), tenderIDs...).Err(); err != nil {
			return err
		}
		if err := c.SAdd(ctx, key.TendersSetKey(t.Destination), tenderIDs...).Err(); err != nil {
			return err
		}
	}

	// Add source and dest to tills set
	if err := c.SAdd(ctx, key.TillsSetKey(), t.Source, t.Destination).Err(); err != nil {
		return err
	}

	return nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	client := Client{rdb}

	// HSET org:test-org:eu:test-eu:date:08-01-2023:till:till-1:tender:tender-1:denomination:$5 bill {"amount":,"count":}
	// SADD org:test-org:eu:test-eu:date:08-01-2023:till:till-1:tender:tender-1:denominations "$5 bill" "$10 bill"
	// SADD org:test-org:eu:test-eu:date:08-01-2023:tills till-1 till-2
	// SADD org:test-org:eu:test-eu:date:08-01-2023:till:till-1 tender-1 tender-2

	k := Key{
		Organization:    "test-org",
		EnterpriseUnit:  "test-eu",
		SettlementDocID: "settlement-id-1",
	}

	transactions := []Transaction{
		{
			Org:             k.Organization,
			EU:              k.EnterpriseUnit,
			SettlementDocID: k.SettlementDocID,
			Source:          "till-1",
			Destination:     "till-2",
			Direction:       ">",
			Tenders: []Tender{
				{
					ID:     "cash",
					Amount: 1.5,
					TenderBreakdowns: []TenderInfo{
						{
							Name:   "dollar bill",
							Count:  1,
							Amount: 1,
						},
						{
							Name:   "quarter",
							Count:  2,
							Amount: 0.5,
						},
					},
				},
			},
		},
		{
			Org:             k.Organization,
			EU:              k.EnterpriseUnit,
			SettlementDocID: k.SettlementDocID,
			Source:          "till-2",
			Destination:     "till-3",
			Direction:       ">",
			Tenders: []Tender{
				{
					ID:     "cash",
					Amount: 1.5,
					TenderBreakdowns: []TenderInfo{
						{
							Name:   "dollar bill",
							Count:  1,
							Amount: 1,
						},
						{
							Name:   "quarter",
							Count:  2,
							Amount: 0.5,
						},
					},
				},
			},
		},
	}

	for _, tx := range transactions {
		if err := client.ProcessTransaction(tx); err != nil {
			panic(err)
		}
	}

	tills, err := client.GetExpectedTenders(context.Background(), k)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", tills)
}
