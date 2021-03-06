from datetime import datetime
from functools import reduce
from urllib.parse import urlparse



class AlbAccessLogEntry:
    COLUMNS = [
		("type", str, ),
		("timestamp", str, ),
		("alb", str, ),
		("client_ip", [str, lambda ip: ip.split(':')], ),
		("backend_ip", [str, lambda ip: ip.split(':')], ),
		("request_processing_time", float, ),
		("backend_processing_time", float, ),
		("response_processing_time", float, ),
		("alb_status_code", int, ),
		("backend_status_code", int, ),
		("received_bytes", int, ),
		("sent_bytes", int, ),
		("request", [str, lambda request: request.split(' ')], ),
		("user_agent", str, ),
		("ssl_cipher", str, ),
		("ssl_protocol", str, ),
		("target_group_arn", str, ),
		("trace_id", str, ),
		("domain_name", str, ),
		("chosen_cert_arn", str, ),
		("matched_rule_priority", int, ),
		("request_creation_time", str, ),
		("actions_executed", str, ),
		("redirect_url", str, ),
    ]

    @classmethod
    def column_name_to_index(cls, name: str) -> int:
        return next(i for i, (column_name, column_type) in enumerate(cls.COLUMNS) if column_name == name)

    @classmethod
    def hydrate(cls, row_data: list):
        return [column_ty(column_data) if callable(column_ty) else reduce(lambda carry, item: item(carry), column_ty, column_data) for (column_name, column_ty), column_data in zip(cls.COLUMNS, row_data)]

    @classmethod
    def hydrate_dict(cls, row_data: list):
        return { column_name: column_ty(column_data) if callable(column_ty) else reduce(lambda carry, item: item(carry), column_ty, column_data) for (column_name, column_ty), column_data in zip(cls.COLUMNS, row_data)}