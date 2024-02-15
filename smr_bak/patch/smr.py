def NewBytes(b):
	"""NewBytes(b bytes) object"""
	return Bytes(handle=_smr.smr_NewBytes(b, len(b)))