vxfs
==============

`vxfs`  A light weight network file system

## Role

### Store Server

It default bind in ":1730", use "-vxfsAddress port" for modify.

Usage
```
vxfs-stored <data store path> <index store path>
```

Example
```bash
./vxfs-stored /data/store1/data /data/store1/index
```

### Name Server

It default bind in ":1720", use "-vxfsAddress port" for modify.

Usage
```
vxfs-named <data store path>
```

Example
```bash
./vxfs-named /data/name
```

### Proxy Server

It default bind in ":1750", use "-vxfsAddress port" for modify.

Usage
```
vxfs-proxyd <machine id> <name server> <store server list>
```

> the **machine id**  was used for generate **snowflake** key.

Example
```bash
./vxfs-proxyd 1 127.0.0.1:1720 1/127.0.0.1:1730
./vxfs-proxyd 1 127.0.0.1:1720 1/127.0.0.1:1730,2/127.0.0.1:1731
```

> **Store Server list** - **"&lt;store id&gt;/&lt;store server address&gt;,..."**, the **store id** **Cannot Modified** when it flag a **Store Server**.

## API

The `vxfs` **Proxy Server** use the **HTTP REST API** for **mostly usage**.

Success reponse like:

``` json
{
    "code": 0,
    "data": {
        ... more fields ...
    }
}
```

Error response like:
``` json
{
    "code": 102,
    "error": "name not exists"
}
```

> It set the **HTTP Status Code** when **Error Response**.

### File Storage

#### Upload File

Request
``` bash
curl -X PUT \
  http://127.0.0.1:1750/logo.png \
  -H 'Content-Type: image/png' \
  --upload-file ./logo.png
```

> The HTTP header `Content-Type` was recommend.

Response
``` json
{
    "code": 0,
    "data": {
    }
}
```

#### Request (GET) File

It can opened in web browser

``` bash
curl -I http://127.0.0.1:1750/logo.png
```

#### Delete File


##### Request

``` bash
curl -X DELETE \
  http://127.0.0.1:7119/logo.png
```

##### Response

``` json
{
    "code": 0,
    "data": {
    }
}
```

## Caveats & Limitations

* The `vxfs` never **recovery** disk space. When **deleting** a file, it simply flag the **file path** and **store data** to delete.
