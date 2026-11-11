# Format des messages Kafka (contrat entre Lot A et Lot B)

Topic : `transactions` — un message JSON par ligne de facture.

```json
{
  "InvoiceNo": "536365",
  "StockCode": "85123A",
  "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
  "Quantity": 6,
  "InvoiceDate": "2010-12-01 08:26:00",
  "UnitPrice": 2.55,
  "CustomerID": "17850",
  "Country": "United Kingdom"
}
```

Règles :
- `InvoiceDate` au format `yyyy-MM-dd HH:mm:ss`
- `CustomerID` peut être `null`
- Les annulations gardent leur `InvoiceNo` commençant par `C`
- Tout changement de ce format doit être validé par les 3 membres
