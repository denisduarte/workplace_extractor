# Workplace Extractor
<p style="float: right;">
  <b>Python package to extract posts from</b>
  <img src="https://raw.githubusercontent.com/denisduarte/midia/main/workplace.png" width="100" height="24">
  <img src="https://raw.githubusercontent.com/denisduarte/midia/main/from_facebook.png" width="100" height="9">
</p>

# Overview
The Workplace Extractor package was written to allow a complete extraction of posts form a Workplace installation. It provides the following key features:

* Access to the SCIM and GRAPH API provided by Facebook
* Asyncronous calls to increase speed
* Posts are exported to a CSV file

# Usage
In the following paragraphs, I am going to describe how you can get and use Scrapeasy for your own projects.

### Installation
To get the Workplace Extractor package, either fork this github repo or use Pypi via pip.
```sh
$ pip install workplace_extractor
```
### How to use it

First, you should import the `Extractor` class from the package, then create an extractor object

```sh
from workplace_extractor import Extractor

wp_extractor = Extractor(token, since, until, csv, loglevel)
```

The `Extractor` class expects the following parameters:

* **token** - path to a file containing the access token
* **since** - the starting date for the extraction. Use the YYYY-MM-DD format.
* **until** - the ending date (exclusive) for the extraction. Use the YYYY-MM-DD format.
* **csv** - path of the CSV file that will be created at the end of the extraction
* **loglevel** - the lev used by `logging`. Should be one of ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# Warning
As many http calls are made during the export process, your program may take a while to finish, depending on the size of your Workplace installation. As a reference, on a installation with around 85,000 users, 3,000 groups and 110,000 posts the exectution takes around 4 hours to complete.

# License

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Copyright 2021 Denis Duarte

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

