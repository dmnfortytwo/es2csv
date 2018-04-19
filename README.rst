============
es2plaintext
============

This is a modified version of original es2csv - a cli utility for querying Elasticsearch and exporting results to plain text (originally it exported to csv files).

Why? Because I don't need cvs, my goal is to get back logs which were pushed to Elasticsearch. Because some tools like ausearch (a part of audit tools) cannot fetch data from ES.

I also removed progressbar, because less dependencies is better.

Support of date range (it uses @timestamp field) will help you to fetch data without writing large Lucene queries

Removed:
--------
- csv support (surprise!), you can export to generic txt file only without string escape
- progressbar. dummy version added instead

Added:
------
- query range support. --from <date> (default to 1970-01-01) and --to <date> (default now)

Requirements
------------
| This tool should be used with Elasticsearch 5.x version, but it works fine with my 6.2.2.
| You also need `Python 2.7.x <https://www.python.org/downloads/>`_ and `pip <https://pip.pypa.io/en/stable/installing/>`_.

Installation
------------
From source:

.. code-block:: bash

    $ pip install git+https://github.com/dmnfortytwo/es2plaintext.git

Usage
-----
.. code-block:: bash

 $ es2plaintext [-h] -q QUERY [-u URL] [-a AUTH] [-i INDEX [INDEX ...]]
          [-D DOC_TYPE [DOC_TYPE ...]] [-t TAGS [TAGS ...]] -o FILE
          [-f FIELDS [FIELDS ...]] [-S FIELDS [FIELDS ...]] [-d DELIMITER]
          [-m INTEGER] [-s INTEGER] [-k] [-r] [-e] [--verify-certs]
          [--ca-certs CA_CERTS] [--client-cert CLIENT_CERT]
          [--from <datetime>] [--to <datetime>]
          [--client-key CLIENT_KEY] [-v] [--debug]

 Arguments:
  -q, --query QUERY                        Query string in Lucene syntax.               [required]
  -o, --output-file FILE                   CSV file location.                           [required]
  -u, --url URL                            Elasticsearch host URL. Default is http://localhost:9200.
  -a, --auth                               Elasticsearch basic authentication in the form of username:password.
  -i, --index-prefixes INDEX [INDEX ...]   Index name prefix(es). Default is ['logstash-*'].
  -D, --doc-types DOC_TYPE [DOC_TYPE ...]  Document type(s).
  -t, --tags TAGS [TAGS ...]               Query tags.
  -f, --fields FIELDS [FIELDS ...]         List of selected fields in output. Default is ['_all'].
  -S, --sort FIELDS [FIELDS ...]           List of <field>:<direction> pairs to sort on. Default is [].
  -d, --delimiter DELIMITER                Delimiter to use in CSV file. Default is ",".
  -m, --max INTEGER                        Maximum number of results to return. Default is 0.
  -s, --scroll-size INTEGER                Scroll size for each batch of results. Default is 100.
  -k, --kibana-nested                      Format nested fields in Kibana style.
  -r, --raw-query                          Switch query format in the Query DSL.
  -e, --meta-fields                        Add meta-fields in output.
  --verify-certs                           Verify SSL certificates. Default is False.
  --ca-certs CA_CERTS                      Location of CA bundle.
  --client-cert CLIENT_CERT                Location of Client Auth cert.
  --client-key CLIENT_KEY                  Location of Client Cert Key.
  --from RANGE_FROM                        Timestamp range: from (ex: 2018-04-18T23:00)
  --to RANGE_TO                            Timestamp range: to (ex: 2018-04-18T23:00 or 2018-04-18 or now)
  -v, --version                            Show version and exit.
  --debug                                  Debug mode on.
  -h, --help                               show this help message and exit

[ `Usage Examples <./docs/EXAMPLES.rst>`_ | `Release Changelog <./docs/HISTORY.rst>`_ ]
