from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

def create_table ():
    schema = Schema(
        NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=5,
            name="details",
            field_type=StructType(
                NestedField(
                    field_id=4, name="created_by", field_type=StringType(), required=False
                ),
            ),
            required=False,
        ),
    )
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )
    # Sort on the symbol
    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

    catalog.create_table(
        identifier="docs_example.bids",
        schema=schema,
        location="s3://pyiceberg",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )

if __name__ == "__main__":
    print("Running locally")
    from pyarrow.fs import LocalFileSystem

    catalog = Catalog(LocalFileSystem("/tmp/pyiceberg"))
    create_table(catalog)
    print("Done")