"""Streams metadata."""
# -*- coding: utf-8 -*-
from types import MappingProxyType

# Streams metadata
STREAMS: MappingProxyType = MappingProxyType(
    {
        "customers": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "customer_bank_accounts": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "events": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "instalment_schedules": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "mandates": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "payments": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "payouts": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
            "child": "payout_items"
        },
        "payout_items": {
            "key_properties": "payout",
            "is_child": True
        },
        "refunds": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "subscriptions": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
        "webhooks": {
            "key_properties": "id",
            "replication_method": "FULL_TABLE",
            "replication_key": "created_at[gt]",
            "bookmark": "created_at",
        },
    }
)
