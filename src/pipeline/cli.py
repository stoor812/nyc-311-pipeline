import click
import logging
from src.pipeline.ingest import fetch_data, save_bronze
from src.pipeline.transform import transform_data, save_silver
from src.pipeline.validate import validate
from src.pipeline.load import get_engine, create_table, upsert


@click.group()
def cli():
    pass


@cli.command()
@click.option("--limit", default=5000, help="Max records to fetch")
def run(limit):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    log = logging.getLogger(__name__)

    if limit <= 0:
        log.error("--limit must be a positive integer")
        raise SystemExit(1)

    # ingest
    try:
        log.info(f"Fetching up to {limit} records from API...")
        df = fetch_data(max_records=limit)
        log.info(f"Fetched {len(df)} records")
        bronze_path = save_bronze(df)
        log.info(f"Saved bronze to {bronze_path}")
    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        raise SystemExit(1)

    # transform
    try:
        log.info("Transforming data...")
        df = transform_data(df)
        silver_path = save_silver(df)
        log.info(f"Saved silver to {silver_path}")
    except Exception as e:
        log.error(f"Transform failed: {e}")
        raise SystemExit(1)

    # validate
    log.info("Validating data...")
    try:
        validate(df)
        log.info("Validation passed")
    except ValueError as e:
        log.error(f"Validation failed: {e}")
        raise SystemExit(1)

    # load
    try:
        log.info("Loading data into PostgreSQL...")
        engine = get_engine()
        create_table(engine)
        upsert(df, engine)
        log.info("Pipeline complete.")
    except Exception as e:
        log.error(f"Load failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    cli()