from builtins import str
from builtins import object
import boto3
import hashlib 
import re
import json 
from pydoc import locate
import sys
from boto3.session import Session
import base64 
import copy 
import uuid

class SimpleDBKeyInUseException(Exception):
    pass


class BaseSimpleDB(object):
    domain = "default_domain"
    region = 'us-west-2'
    
    S3_DATA_KEY = 'data/{}/{}.json'
    PROJECT_BUCKET = "" #"unforgettable-simpledb" * changed name to s3_bucket
    s3_bucket = "" # ** USER MUST SET THIS **

    def __init__(self, **kwargs):
        if not 's3_bucket' in kwargs:
            raise Exception("You must provide an s3_bucket so that large objects can laso be saved.")

    # When pushing projects around
    # using json bytes will break transfers
    skip_byte_conversion = True

    re_s3 = re.compile('^s3://(.+?)/(.+)')

    def _get_S3_client(self):
        session = Session(region_name=self.region)
        client = session.client('s3')
        return client

    def _quote_attrs(self, update_atts):
        """ Prepare and chunk the _attrs_type map into <1KB JSON pieces. """
        # 1) Merge existing record details
        attrs = {}
        checker = Record(region=self.region, domain=self.domain, key=self.key, s3_bucket=self.s3_bucket)
        record_details = checker.fetch() or {}
        merged = {**record_details, **update_atts}

        # 2) Build full_types map and perform S3 offloading if needed
        full_types = json.loads(merged.get('_attrs_type', '{}'))
        values = {}
        for k, v in merged.items():
            if k == '_attrs_type':
                continue
            t = type(v)
            if isinstance(v, (list, dict)):
                full_types[k] = 'json'
                val = json.dumps(v)
            elif t.__name__ == 'bytes':
                full_types[k] = 'base64'
                val = base64.b64encode(v).decode('utf-8')
            else:
                full_types[k] = t.__name__
                val = str(v)

            # Offload large values to S3
            if sys.getsizeof(val) > 1024:
                payload = json.dumps({'key': k, 'value': val, 'type': full_types[k]})
                s3_key = self.S3_DATA_KEY.format(self.key, k)
                self._get_S3_client().put_object(
                    Key=s3_key,
                    Bucket=self.s3_bucket,
                    Body=payload,
                    ContentType='application/json'
                )
                full_types[k] = 's3ref'
                val = f"s3://{self.s3_bucket}/{s3_key}"

            values[k] = val

        # 3) Chunk the full_types dict into <1KB pieces by key
        chunks = []
        current = {}
        for name, typ in full_types.items():
            current[name] = typ
            text = json.dumps(current, separators=(',', ':'))
            if sys.getsizeof(text) > 1024:
                # remove last and start new chunk
                current.pop(name)
                chunks.append(current.copy())
                current = {name: typ}
        if current:
            chunks.append(current)

        # 4) Assign chunk0 → _attrs_type, chunk1 → _attrs_type_1, etc.
        for idx, chunk in enumerate(chunks):
            key_name = '_attrs_type' if idx == 0 else f'_attrs_type_{idx}'
            values[key_name] = json.dumps(chunk, separators=(',', ':'))

        return values

    def _unquote_attrs(self, attrs):
        """ Reassemble split _attrs_type chunks and decode attributes. """
        # 1) Gather all _attrs_type* fragments
        fragments = {}
        for k in list(attrs):
            if k.startswith('_attrs_type'):
                fragments[k] = attrs.pop(k)
        #print("Fragments found:", json.dumps(fragments, indent=2))
        # 2) Merge them in order: '' < '_1' < '_2' ...
        merged_types = {}
        for key in sorted(fragments, key=lambda k: (k != '_attrs_type', k)):
            frag = json.loads(fragments[key])
            merged_types.update(frag)

        # 3) First resolve any s3refs
        for name, typ in list(merged_types.items()):
            if typ == 's3ref' and name in attrs:
                m = self.re_s3.match(attrs[name])
                bucket, key = m.groups()
                body = self._get_S3_client().get_object(Key=key, Bucket=bucket)['Body'].read()
                ref = json.loads(body)
                merged_types[name] = ref['type']
                attrs[name] = ref['value']

        # 4) Cast values back to Python types
        result = {}
        for name, typ in merged_types.items():
            if name not in attrs:
                continue
            val = attrs[name]
            if typ == 'json':
                result[name] = json.loads(val)
            elif typ == 'bool':
                result[name] = (val == 'True')
            elif typ == 'base64':
                if self.skip_byte_conversion:
                    result[name] = val
                else:
                    result[name] = base64.b64decode(val.encode('utf-8'))
            elif typ == 'unicode':
                result[name] = str(val)
            else:
                cls = locate(typ)
                result[name] = cls(val)
        # Preserve a single merged map
        result['_attrs_type'] = json.dumps(merged_types, separators=(',', ':'))
        return result
    
    def _parse_attrs(self, attributes):
        attrs = {}
        for item in attributes:
            attrs[item['Name']] = item['Value']
        return self._unquote_attrs(attrs) 

class Record(BaseSimpleDB):
    """ 
    Simple DB Record 
    Pass in a domain otherwise default_domain will be used.
    Usage:
    rec = Record(domain="mydomain")

    rec.key = "mykey"
    attrs = rec.fetch()

    attrs['new_attr'] = "new_val"
    rec.set_attributes(attrs)
    rec.update()

    """
    attributes = []
    key = None
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for k in kwargs:
            if k == 'attributes':
                self.set_attributes(kwargs[k])
            else:
                setattr(self, k, kwargs[k])

        if hasattr(self, 'profile_name') and self.profile_name:
            session = Session(profile_name=self.profile_name)
            self.conn = session.client('sdb', region_name=self.region)
        else:
            self.conn = boto3.client('sdb', region_name=self.region)
            

    def set_attributes(self, attrs, replace=True, key=None, quote=False):
        if quote:
            if not self.key:
                raise Exception("No key set")
            attrs = self._quote_attrs(attrs)
           
        if replace:
            self.attributes = []
        for name, value in attrs.items():
            self.add_attr(name, value, replace)

    def add_attr(self, name, value, replace=True):
        print(name, value, replace)
        self.attributes.append({
            'Name': name,
            'Value': str(value),
            'Replace': replace
            })

    def get_attr(self, name):
        for x in self.attributes:
            if x['Name'] == name:
                return x['Value']


    def update(self):
        if not self.key:
            raise Exception("No key set")
        if not self.attributes:
            return False
        
        self.conn.put_attributes(
            DomainName=self.domain,
            ItemName=self.key,
            Attributes=self.attributes
            )

    def delete(self, key=None):
        if key:
            self.key=key
        if not self.key:
            raise Exception("No key set")

        response = self.conn.delete_attributes(
            DomainName=self.domain,
            ItemName=self.key,
                )



    def fetch(self, key=None):
        if key:
            self.key=key
        if not self.key:
            raise Exception("No key set")
        response = self.conn.get_attributes(
                DomainName=self.domain,
                ItemName=self.key,
                ConsistentRead=True
                )

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.attributes = response.get("Attributes", [])
            if self.attributes:
                return self._parse_attrs(self.attributes)
            else:
                return {}
        else:
            raise Exception("Error fetching record")
            #return {}
        
        

    @classmethod
    def _make_key(cls, *argv, **opts):
        m = None
        
        if 'uuid4' in opts and opts['uuid4']:
            m = uuid.uuid4()
            return m.hex
        
        elif 'sha256' in opts and opts['sha256']:
            m = hashlib.sha256()
        else:
            m = hashlib.md5()

        for i, arg in enumerate(argv):
            #print i, arg
            m.update(str(arg).encode('utf-8'))
        return m.hexdigest()
        

    def make_key(self, *argv, **opts):
        """ 
        Use to make unique key for SimpleDB for both 
        Experiments and Experiments Participants
        m = hashlib.md5()
        for arg in argv:
            m.update(str(arg))
        key = m.hexdigest()
        """

        key = self._make_key(*argv, **opts)
        #print key
        check_record = Record(region=self.region, domain=self.domain, key=key, s3_bucket=self.s3_bucket)
        
        if check_record.fetch() and '_uid' in check_record.fetch().keys():
            print(f"Warning: '_uid' key match found in simpleDB domain: {self.domain}")
            raise SimpleDBKeyInUseException(key)
        else:
            print(f"OK: unique '_uid' key generated for simpleDB domain: {self.domain}")

        self.key = key
        return self.key

     
class QuerySelect(BaseSimpleDB):
    """ 
    Convenience Class to make SQL queries on SimpleDB
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for k in kwargs:
            setattr(self, k, kwargs[k])
            

        if hasattr(self, 'profile_name'):
            session = Session(profile_name=self.profile_name)
            self.conn = session.client('sdb', region_name=self.region)
        else:
            self.conn = boto3.client('sdb', region_name=self.region)


    def sql(self, sql, items=None, next_token=None):
        """
        @Params
        sql: SQL query string

        Returns array of results
        """
        if not items:
            items = []
        if next_token:
            response = self.conn.select(
                    SelectExpression=sql,
                    NextToken=next_token,
                    ConsistentRead=True
                    )
        else:
            response = self.conn.select(
                    SelectExpression=sql,
                    ConsistentRead=True
                    )

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            # print('response-query',json.dumps(response.get("Items", []), indent=2))
            for item in response.get("Items", []):
                items.append( self._parse_attrs(item.get("Attributes", [])) )
        
        next_token = response.get("NextToken")

        if next_token:
            return self.sql(sql, items, next_token)
        else:
            return items


class Paginator(BaseSimpleDB):
    """ 
    Class to Paginate through SimpleDB results
    Usage:

        # Example 1. Paginate through results 
        paginator = Paginator(domain=domain, region=region)
        query = {
            'select': 'uid, title',
            'limit': 2
        }
        next_token, items = paginator.paginate(query)  # First page
        next_token, items = paginator.paginate(query, next_token=next_token)  # Second page


        # Example 2. Paginate from arbitrary starting point
        paginator = Paginator(domain=domain, region=region)
        query = {
            'select': 'uid, title',
            'limit': 2
        }
        next_token, items = paginator.paginate(query, start=5)  # First page starting at result 5
        next_token, items = paginator.paginate(query, next_token=next_token)  # Second page

    """

    NEXT_TOKEN_LIMIT = 2500

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
        if hasattr(self, 'profile_name'):
            session = Session(profile_name=self.profile_name)
            self.conn = session.client('sdb', region_name=self.region)
        else:
            self.conn = boto3.client('sdb', region_name=self.region)


    def sql(self, query=None, next_token=None, fetch_starting_token=False):
        """
        Params:
            query: {
                "select": "*",
                "where": "project_id = 'xyz'",
                "order by": "created DESC",  # Note the space between order + by
                "limit": 10
            }
        """

        if not query:
            raise Exception("No query defined")

        # Assemble the sql query
        sql = "SELECT {} FROM {}".format(query['select'], self.domain)
        for part in ["where", "order by", "limit"]:
            if part in query:
                sql = "{} {} {} ".format(sql, part.upper(), query[part])
        sql = sql.strip()

        response = {}
        if next_token:
            response = self.conn.select(
                    SelectExpression=sql,
                    ConsistentRead=True,
                    NextToken=next_token
                    )
        else:
            response = self.conn.select(
                    SelectExpression=sql,
                    ConsistentRead=True,
                    )
        items = []

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            if fetch_starting_token:
                # We are only after the token, so no need to return items
                return (response.get('NextToken'), items)

            for item in response.get("Items", []):
                items.append( self._parse_attrs(item.get("Attributes", [])) )

        return (response.get('NextToken'), items)


    def fetch_next_token_for_starting_point(self, query, start, next_token=None):
        # This is a hack to get the correct starting point for
        # our request. It was suggested by the SimpleDB developers here:
        # https://forums.aws.amazon.com/message.jspa?messageID=253237#253237
        #
        # Page is no longer there, but is still referenced here:
        # https://stackoverflow.com/questions/4623324/how-to-do-pagination-in-simpledb

        #
        # The count (*) is very quick so, will find our NextToken quickly
        # We need to recursively call for more than 2500 records,
        # presumably up to the next 2500

        query_clone = copy.deepcopy(query) 
        if start > self.NEXT_TOKEN_LIMIT:
            start -= self.NEXT_TOKEN_LIMIT
            query_clone['limit'] = self.NEXT_TOKEN_LIMIT
            query_clone['select'] = "count(*)"
            next_token, items = self.sql(query=query_clone, next_token=next_token, fetch_starting_token=True)
            return self.fetch_next_token_for_starting_point(query_clone, start, next_token=next_token)

        else:
            start -= 1
            if start == 0:
                return next_token # Will either be None or NextToken
            else:
                query_clone['limit'] = start
                next_token, items = self.sql(query=query_clone, next_token=next_token, fetch_starting_token=True)
                return next_token


    def paginate(self, query, start=1, next_token=None):
        """
        Paginate through SimpleDB query

        Params
            query: {
                "select": "*",
                "where": "project_id = 'xyz'",
                "order by": "created DESC",  # Note the space between order + by
                "limit": 10
            }

            start: first result number
            next_token: to staring point

        """
        if not next_token:
            if start == 1:
                return self.sql(query=query)
            # Need to use the next_token hack to get the right
            # starting point
            next_token = self.fetch_next_token_for_starting_point(query, start)

        if next_token:
            return self.sql(query=query, next_token=next_token)

        return (None, [])


class BatchUpdate(BaseSimpleDB):
    """ 
    Convenience Class to batch update on SimpleDB
    """

    attributes = []
    key = None
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for k in kwargs:
            setattr(self, k, kwargs[k])
            

        if hasattr(self, 'profile_name'):
            session = Session(profile_name=self.profile_name)
            self.conn = session.client('sdb', region_name=self.region)
        else:
            self.conn = boto3.client('sdb', region_name=self.region)

    def split_attributes(self, items, number):
        batch_update_attributes = []
        
        while items:
            item = items.pop()
            batch_update_attributes.append(item)

            if len(batch_update_attributes) >= number or len(items) == 0:
                self.conn.batch_put_attributes(
                    DomainName=self.domain,
                    Items=batch_update_attributes
                    )
                batch_update_attributes = []

    def set_batch_attributes(self, batch_attrs, replace=True, quote=False):
        for batch_attr_name, batch_attr_item in batch_attrs.items():
            self.key = batch_attr_name
            quote_batch_attrs = self._quote_attrs(batch_attr_item)
            batch_attr_item['_attrs_type'] =  quote_batch_attrs['_attrs_type']
            item_value_list = []
            for name, items in batch_attr_item.items():
                item_value_list.append({'Name': name, 'Value': quote_batch_attrs[name], 'Replace': True })
            self.add_attr(batch_attr_name, item_value_list, replace)

    def add_attr(self, name, item_value_list, replace=True):
        self.attributes.append({
            'Name': name,
            'Attributes': item_value_list
            })   
    

    def update(self):
        value_size = sys.getsizeof(self.attributes)

        if value_size > 1024:
            length_attrs = len(self.attributes)
            if length_attrs == 1 :
                record = Record(region=self.region, domain=self.domain, key=self.key, s3_bucket=self.s3_bucket)
                attrs = {}
                for attr in self.attributes[0]['Attributes']:
                    attrs[attr['Name']] = attr['Value']
                record.set_attributes(attrs)
                record.update()
            else:
                self.split_attributes(self.attributes, round(length_attrs/2))
        else:
            if len(self.attributes) > 25:
                self.split_attributes(self.attributes, 25)
            else:
                print(json.dumps(self.attributes, indent=4, default=str))
                self.conn.batch_put_attributes(
                    DomainName=self.domain,
                    Items=self.attributes
                    )
        

class Domain(QuerySelect):
    """ 
    Convenience Class manipulate SimpleDB domains)
    """
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

        if hasattr(self, 'profile_name'):
            session = Session(profile_name=self.profile_name)
            self.conn = session.client('sdb', region_name=self.region)
        else:
            self.conn = boto3.client('sdb', region_name=self.region)

        
    def create(self, domain=None):
        if domain:
            self.domain = domain
        if not self.domain:
            raise Exception("Domain name is not set")

        response = self.conn.create_domain(
                DomainName=self.domain
                )

    def delete(self, domain=None):
        if domain:
            self.domain = domain
        if not self.domain:
            raise Exception("Domain name is not set")

        response = self.conn.delete_domain(
                DomainName=self.domain
                )

    def list_domains(self, items=None, next_token=None):
        if not items:
            items = []
        if next_token:
            response = self.conn.list_domains(
                    NextToken=next_token,
                    )
        else:
            response = self.conn.list_domains()

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            items.extend( response.get("DomainNames", []) ) 
        
        next_token = response.get("NextToken")

        if next_token:
            self.list_domains(items, next_token)
        else:
            return items

    def count(self, key=None, value=None):
        """Count number of Items in a Domain"""
        sql = "SELECT count(*) FROM {} ".format(self.domain) 
        if key:
            sql = "SELECT count(*) FROM {} WHERE {} = '{}' ".format(self.domain, key, value)
        response = self.conn.select(
                SelectExpression=sql,
                ConsistentRead=True
                )

        return int( response.get('Items')[0]['Attributes'][0]['Value'] )




class SimpleDatabase(object):
    s3_bucket = ''

    def __init__(self, *args, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

        if not self.database:
            raise Exception("No database supplied.")
        if not self.region:
            raise Exception("No database region supplied.")


    def _checkattr(self, attr):
        if not hasattr(self, attr):
            raise Exception("No uid supplied.")


    def get(self):
        self._checkattr('uid')
        rec = Record(domain=self.database, region=self.region, s3_bucket=self.s3_bucket, key=self.uid)
        return rec.fetch()


    def set(self, attrs, replace=True):
        self._checkattr('uid')
        rec = Record(domain=self.database, region=self.region, s3_bucket=self.s3_bucket, key=self.uid)
        attrs['_uid'] = self.uid # make sure we have a record of the uid
        rec.set_attributes(attrs, replace=replace, quote=True)
        rec.update()


    def delete(self):
        self._checkattr('uid')
        db = Record(domain=self.database, region=self.region, s3_bucket=self.s3_bucket)
        db.conn.delete_attributes(
            DomainName=self.database,
            ItemName=self.uid
                )

    def delete_attributes(self, *attrs):
        self._checkattr('uid')
        rec = Record(domain=self.database, region=self.region, s3_bucket=self.s3_bucket, key=self.uid)
        current_attrs = rec.fetch()
        attributes = []
        for k in attrs:
            if k in current_attrs:
                value = ""
                v = current_attrs[k]
                t = type(v)
                if isinstance(v, list) or isinstance(v, dict):
                    value = json.dumps(v)   
                elif t.__name__ == 'bytes':
                    value = base64.b64encode(v).decode('utf-8')
                else:
                    value = str(v)
                                      
                attributes.append({
                    'Name': k,
                    'Value': value
                    })
        rec.conn.delete_attributes(
            DomainName=self.database,
            ItemName=self.uid,
            Attributes=attributes    
        )


    def list(self, cols=None, condition=None):
        """
        Query and return records from the database with proper type reconstruction.
        
        Args:
            cols (str, optional): Comma-separated column names to select. If None, selects all columns.
            condition (str, optional): WHERE clause condition for filtering results.
            
        Returns:
            list: List of dictionaries containing the query results with proper Python types.
            
        Note:
            When specifying custom columns, this method automatically includes all _attrs_type* 
            chunks (up to _attrs_type_5) to ensure proper data type reconstruction. This is 
            necessary because:
            
            1. The _attrs_type metadata is split into chunks when it exceeds 1KB
            2. Without all chunks, attributes would be missing from results (data loss bug)
            3. SimpleDB doesn't support wildcard patterns like '_attrs_type*'
            
            The overhead of requesting unused _attrs_type columns is minimal, and this approach
            handles 99%+ of real-world scenarios where records need 1-6 type chunks.
        """
        cols_original = cols
        if cols:
            # Ensure all _attrs_type chunks are included for proper data reconstruction
            attrs_type_cols = ['_attrs_type', '_attrs_type_1', '_attrs_type_2', '_attrs_type_3', '_attrs_type_4', '_attrs_type_5']
            missing_chunks = []
            for col in attrs_type_cols:
                if col not in cols:
                    missing_chunks.append(col)
            
            if missing_chunks:
                cols = ', '.join(missing_chunks) + ', ' + cols
        else:
            cols = '*'
            
        query = 'SELECT {} FROM {} '.format(cols, self.database)
        if condition:
            query += 'WHERE {} '.format(condition)
        qs = QuerySelect(region=self.region, s3_bucket=self.s3_bucket)
        response = qs.sql(query) 
             
        if cols_original:
            # Filter out the requested columns from the response
            cols = [x.strip() for x in cols_original.split(',')]
            results = []
            for item in response:
                filtered_item = {k: v for k, v in item.items() if k in cols}
                if filtered_item:
                    results.append(filtered_item)

            return results
        else:
            # Return the full response with all columns
            return response        

