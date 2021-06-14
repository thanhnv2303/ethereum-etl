import aerospike

from config.config import AerospikeDBConfig

config = {
    'hosts': [('0.0.0.0', 3000)]
}

try:
    client = aerospike.client(config).connect()
except Exception as e:
    import sys

    print(e)
    print("failed to connect to the cluster with", config['hosts'])
    sys.exit(1)

# Records are addressable via a tuple of (namespace, set, key)
key = ('test', 'demo', 'foo')

try:
    # Write a record
    client.put(key, {
        'name': 'John Doe',
        'age': 32
    })
except Exception as e:
    import sys

    print("error: {0}".format(e), file=sys.stderr)

# Read a record
key = ('test', 'demo', 'foo')
(key, metadata, record) = client.get(key)
print(record)

# Close the connection to the Aerospike cluster

key = (AerospikeDBConfig.NAMESPACE,AerospikeDBConfig.WALLET_SET)

scan = client.scan(AerospikeDBConfig.NAMESPACE,set=AerospikeDBConfig.WALLET_SET)

res = scan.results()
import pprint
pp = pprint.PrettyPrinter(indent=2)
pp.pprint(res)
client.close()