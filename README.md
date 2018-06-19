# tap-emarsys

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Emarsys's [API](https://help.emarsys.com/hc/en-us/articles/115005253125-Emarsys-API-Endpoints)
- Extracts the following resources from Emarsys
  - [Campaigns](https://help.emarsys.com/hc/en-us/articles/115004522154-Listing-Email-Campaigns)
  - [Contacts](https://help.emarsys.com/hc/en-us/articles/115004494474-Querying-Contact-Data)
  - [Contact Lists](https://developers.hubspot.com/docs/methods/contacts/get_contacts)
  - [Contact Lists](https://help.emarsys.com/hc/en-us/articles/115004496574-Listing-Available-Contact-Lists)
  - [Contact List Memberships](https://help.emarsys.com/hc/en-us/articles/115004467333-Listing-Contacts-in-a-Contact-List)
  - [Email Campaign Metrics](https://help.emarsys.com/hc/en-us/articles/115004494093-Querying-Responses-Result)
      - Daily occurrences of email events
          - Opened
          - Not Opened
          - Received
          - Clicked
          - Not Clicked
          - Bounced
          - Hard Bounced
          - Soft Bounced
          - Block Bounced
- Outputs the schema for each resource

## Configuration

This tap requires a `config.json` which specifies details regarding [API authentication](https://help.emarsys.com/hc/en-us/articles/115004521774-API-Authentication), a cutoff date for syncing historical data, and an optional flag which controls collection of anonymous usage metrics. See [config.sample.json](config.sample.json) for an example.

To run `tap-emarsys` with the configuration file, use this command:

```bash
› tap-emarsys -c my-config.json
```

---

Copyright &copy; 2018 Stitch
