import hashlib,os
salt1 = os.environ.get('SALT1')
salt2 = os.environ.get('SALT2')

def gsa_hash(x):
        x = str(x)
        x = x.lower().strip()
        x = (salt1+x+salt2)
        x = hashlib.sha256(x.encode("utf-8")).hexdigest()
        return x