package events

import (
	"errors"
	"strings"
	"testing"
)

func validInput() Input {
	return Input{
		Type:       "payment.created",
		PaymentID:  "pay_123",
		Amount:     4999,
		Currency:   "EUR",
		CustomerID: "cus_123",
	}
}

func TestValidateAcceptsAndNormalisesValidInput(t *testing.T) {
	in := validInput()
	in.Currency = " eur "
	in.PaymentID = "  pay_123 "

	if err := in.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if in.Currency != "EUR" {
		t.Errorf("currency not upper-cased/trimmed: %q", in.Currency)
	}
	if in.PaymentID != "pay_123" {
		t.Errorf("payment_id not trimmed: %q", in.PaymentID)
	}
}

func TestValidateRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Input)
		field  string
	}{
		{"missing type", func(i *Input) { i.Type = "" }, "type"},
		{"unknown type", func(i *Input) { i.Type = "payment.exploded" }, "type"},
		{"missing payment id", func(i *Input) { i.PaymentID = "" }, "payment_id"},
		{"payment id too long", func(i *Input) { i.PaymentID = strings.Repeat("a", 256) }, "payment_id"},
		{"payment id bad chars", func(i *Input) { i.PaymentID = "pay 123;" }, "payment_id"},
		{"missing customer id", func(i *Input) { i.CustomerID = "" }, "customer_id"},
		{"zero amount", func(i *Input) { i.Amount = 0 }, "amount"},
		{"negative amount", func(i *Input) { i.Amount = -1 }, "amount"},
		{"huge amount", func(i *Input) { i.Amount = maxAmount + 1 }, "amount"},
		{"short currency", func(i *Input) { i.Currency = "EU" }, "currency"},
		{"numeric currency", func(i *Input) { i.Currency = "123" }, "currency"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mutate(&in)
			err := in.Validate()

			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("expected *ValidationError, got %v", err)
			}
			if _, ok := verr.Fields[tc.field]; !ok {
				t.Errorf("expected error on field %q, got %v", tc.field, verr.Fields)
			}
		})
	}
}

func TestValidateReportsAllFieldsAtOnce(t *testing.T) {
	in := Input{}
	err := in.Validate()
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %v", err)
	}
	if len(verr.Fields) != 5 {
		t.Errorf("expected 5 field errors, got %d: %v", len(verr.Fields), verr.Fields)
	}
	if !strings.Contains(verr.Error(), "amount") || !strings.Contains(verr.Error(), "type") {
		t.Errorf("Error() should list fields: %s", verr.Error())
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range []string{"pending", "processing", "succeeded", "failed"} {
		if _, ok := ParseStatus(s); !ok {
			t.Errorf("ParseStatus(%q) should succeed", s)
		}
	}
	if _, ok := ParseStatus("done"); ok {
		t.Error("ParseStatus(\"done\") should fail")
	}
}
