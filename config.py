# Setting byte types to object as it's inferred from the data
# "json" is not a numpy/pandas datatype but we handle it in our script.

tables_config = [
    {
        'name': 'code',
        'datatypes': {
            'code_hash': 'object',
            'code': 'object'
        },
        'chunk_size': 10000,
        'num_chunks_per_file': 10
    },
    {
        'name': 'contracts',
        'datatypes': {
            'id': 'string',
            'creation_code_hash': 'object',
            'runtime_code_hash': 'object'
        },
        'chunk_size': 100000,
        'num_chunks_per_file': 10
    },
    {
        'name': 'contract_deployments',
        'datatypes': {
            'id': 'string',
            'chain_id': 'Int64',
            'address': 'object',
            'transaction_hash': 'object',
            'block_number': 'Int64',
            'transaction_index': 'Int32',
            'deployer': 'object',
            'contract_id': 'string'
        },
        'chunk_size': 100000,
        'num_chunks_per_file': 10
    },
    {
        'name': 'compiled_contracts',
        'datatypes': {
            'id': 'string',
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]',
            'created_by': 'string',
            'updated_by': 'string',
            'compiler': 'string',
            'version': 'string',
            'language': 'string',
            'name': 'string',
            'fully_qualified_name': 'string',
            'sources': 'json',
            'compiler_settings': 'json',
            'compilation_artifacts': 'json',
            'creation_code_hash': 'object',
            'creation_code_artifacts': 'json',
            'runtime_code_hash': 'object',
            'runtime_code_artifacts': 'json'
        },
        'chunk_size': 1000,
        'num_chunks_per_file': 5
    },
    {
        'name': 'verified_contracts',
        'datatypes': {
            'id': 'Int64',
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]',
            'created_by': 'string',
            'updated_by': 'string',
            'deployment_id': 'string',
            'compilation_id': 'string',
            'creation_match': 'bool',
            'creation_values': 'json',
            'creation_transformations': 'json',
            'runtime_match': 'bool',
            'runtime_values': 'json',
            'runtime_transformations': 'json'
        },
        'chunk_size': 100000,
        'num_chunks_per_file': 10
    }
]