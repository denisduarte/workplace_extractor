# Workplace Extractor
<p style="float: right;">
  <b>Python package to extract posts from</b>
  <img src="https://raw.githubusercontent.com/denisduarte/midia/main/workplace.png" width="100" height="24">
  <img src="https://raw.githubusercontent.com/denisduarte/midia/main/from_facebook.png" width="100" height="9">
</p>

# Overview
The Workplace Extractor package was written to allow a complete extraction of posts form a Workplace installation. It provides the following key features:

* Access to the SCIM and GRAPH API provided by Facebook;
* Asyncronous calls to increase speed;
* Lists of **posts**, **members**, **groups**, **comments**, **event attendees** are exported to CSV files;
* A ranking of most relevant members can be created based on the number of interctions (comments and reactions)
* The interaction network can be wxported to a GEXF file;

# Usage

### Installation
To get the Workplace Extractor package, either fork this github repo or use Pypi via pip.
```sh
$ pip install workplace_extractor
```
### How to use it

This package uses [argparse](https://docs.python.org/3/library/argparse.html) and [Gooey](https://github.com/chriskiehl/Gooey) to create an into end-user-friendly GUI application. Just run the app and a dialog will show up asking for the input parameters.

THe application will offer some extraction options:

1. **POSTS** - used for extracting all posts published in a given period of time or feed, from a given author etc.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%201%20-%20Posts.png" width="402" height="337">


2. **Comments** - used for extracting all comments made in a post.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%202%20-%20Comments.png" width="402" height="337">


3. **People** - used for extracting all Workplace users.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%203%20-%20People.png" width="402" height="337">


4. **Groups** - used for all groups.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%204%20-%20Groups.png" width="402" height="337">


5. **Members** - used for extracting all members of a group

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%205%20-%20Members.png" width="402" height="337">

6. **Attendees** - used for extracting all attendees of an event.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%206%20-%20Attendees.png" width="402" height="337">

7. **Interactions** - used for extracting interactions among all workplace users. This option can be used can be used with a network visualization solution, such as [Gephi](https://gephi.org/), for further analysis.

    <img src="https://raw.githubusercontent.com/denisduarte/midia/main/Workplace%20App%207%20-%20Interactions.png" width="402" height="337">


**You must have an access token with full access to both SCIM and GRAPH API in order to the extraction to work**

A config.ini file con be used to set some key parameters. Two required ones are:

* **output_dir** - path the folder where the output will be stored
* **access_token** - path to a file containing the Workplace access token

# Warning
As many http calls are made during the export process, your program may take a while to finish, depending on the size of your Workplace installation. As a reference, on an installation with around 85,000 users, 3,000 groups and 110,000 posts the exectution takes around 4 hours to complete.

# License

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Copyright 2021 Denis Duarte

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

