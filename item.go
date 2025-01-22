package ddbcertstorage

import (
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Item struct {
	Key        string
	Contents   []byte
	Modified   time.Time
	Size       int64
	IsTerminal bool
}

func (i *Item) Item() map[string]types.AttributeValue {
	item := make(map[string]types.AttributeValue)
	item["Key"] = &types.AttributeValueMemberS{Value: i.Key}
	if len(i.Contents) > 0 {
		item["Contents"] = &types.AttributeValueMemberB{Value: i.Contents}
	}
	if !i.Modified.IsZero() {
		item["Modified"] = &types.AttributeValueMemberS{Value: i.Modified.Format(time.RFC3339)}
	}
	if i.Size > 0 {
		item["Size"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(i.Size, 10)}
	}
	if i.IsTerminal {
		item["IsTerminal"] = &types.AttributeValueMemberBOOL{Value: i.IsTerminal}
	}
	return item
}

func (i *Item) Load(item map[string]types.AttributeValue) error {
	if m, ok := item["Key"].(*types.AttributeValueMemberS); ok {
		i.Key = m.Value
	} else {
		return errors.New("invalid attribute value")
	}
	if m, ok := item["Contents"].(*types.AttributeValueMemberB); ok {
		i.Contents = m.Value
	} else {
		return errors.New("invalid attribute value")
	}
	if m, ok := item["Modified"].(*types.AttributeValueMemberS); ok {
		var err error
		i.Modified, err = time.Parse(time.RFC3339, m.Value)
		if err != nil {
			return err
		}
	} else {
		return errors.New("invalid attribute value")
	}
	if m, ok := item["Size"].(*types.AttributeValueMemberN); ok {
		var err error
		i.Size, err = strconv.ParseInt(m.Value, 10, 64)
		if err != nil {
			return err
		}
	} else {
		return errors.New("invalid attribute value")
	}
	if m, ok := item["IsTerminal"].(*types.AttributeValueMemberBOOL); ok {
		i.IsTerminal = m.Value
	} else {
		return errors.New("invalid attribute value")
	}
	return nil
}
