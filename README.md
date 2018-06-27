[![Build Status](https://travis-ci.com/RevoltBI/ex-marketo.svg?branch=master)](https://travis-ci.com/RevoltBI/ex-marketo)
# ex-marketo
Marketo extractor for Keboola Connection

## Description
This is the repository from which the `ex-marketo` extractor for [Marketo REST API](http://developers.marketo.com/rest-api/) is [built automatically](https://travis-ci.com/RevoltBI/ex-marketo).

## Usage
This component will attempt to use bulk exports of `Leads` and `Activities` according to parameters passed via the configuration JSON fragment. The `days` parameter dictates how many days in the past (current day included) the export should go, e.g. one would use the value `1` to export only records where the appropriate datetime parameter has current date is its date part. Everything else should be obvious.

### Configuration example
```json
{
  "destination_bucket": "in.c-marketo",
  "incremental": true,
  "munchkin_id": "123-ABC-456",
  "client_id": "1234213f-4bc2-456a-8981-d725da15ac54",
  "client_secret": "gIXpQBSIOIHcNwV5sNDYXSbyJVPJxDmH",
  "days": 5,
  "leads_date_filter_type": "updatedAt",
  "leads_fields": [
    "id",
    "createdAt",
    "updatedAt",
    "country",
    "MarketoSalesDivisionTHV__c",
    "leadSource",
    "Opt_in_for_emails__c",
    "unsubscribed",
    "emailInvalid",
    "THV_Role__c"
  ],
  "leads_primary_key": ["id"],
  "activities_type_ids": [
    13,
    1
  ],
  "activities_fields": [
    "marketoGUID",
    "leadId",
    "activityDate",
    "activityTypeId",
    "campaignId",
    "primaryAttributeValueId",
    "primaryAttributeValue",
    "attributes"
  ],
  "activities_primary_key": ["marketoGUID"]
}
```
