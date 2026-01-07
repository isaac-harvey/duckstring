from duckstring import Basin, Duck, Catchment

# Duck Types
def duck_local():
    return Duck(
        type="local",
    )

def duck_motherduck():
    return Duck(
        type="cloud",
        provider="aws",
        instance_type="m5.xlarge",
    )

# Catchment
def init_catchment(spec: str):
    # Catchment
    catchment = Catchment()

    # Storage
    catchment.set_storage(
        storage_type="s3",
        bucket_name="my-duckstring-catchment",
        region="us-west-2",
    )

    # Ponds
    ## Ponds in scope are registered against the catchment with their repo path
    catchment.load_ponds("experiment/ponds.json")

    # Ducks
    ## Define Duck instances available to the catchment
    ## First argument is the default duck for all ponds
    ## Second argument is a mapping of pond name to duck for pond-specific overrides
    ## The operation validates that the specified Ponds exist in the Basin, emits a warning if not.
    catchment.set_duck_pools({
        "local": duck_local(),
        "big": duck_motherduck(),
    }, overwrite=False)

    catchment.set_default_duck("local")
    catchment.set_pond_ducks({"order_daily": "big", "order_weekly": "big"}, overwrite=False)


    # Modes
    ## Define execution modes available to the catchment
    catchment.set_modes({
        "weekly": {
            "type": "scheduled",
            "schedule": "0 0 * * 0",  # Every Sunday at midnight
            "freshness": "7d", # Skip execution if data is fresh within 7 days
        },
        "ripple": {
            "type": "ripple",
            "max_concurrent_pulses": 5,
            "max_concurrent_ponds": 10,
            "freshness": "5m", # Skip execution if data is fresh within 5 minutes
        },
    }, overwrite=False)

    # Basins
    ## Specify basins as different objects (they can potentially be split out to their own files if ever needed)
    fast_orders = (
        Basin()
        .set_outlets({
            "order_daily": "1.2.3",
        })
    )

    weekly_rollups = (
        Basin()
        .set_outlets({
            "order_weekly": "1.2.4",
        })
    )

    ## Register basins with the catchment, specifying the duck mode for each basin
    catchment.set_basins({
        "fast_orders": (fast_orders, "ripple"),
        "weekly_rollups": (weekly_rollups, "weekly"),
    }, overwrite=False)

    # Save the catchment
    catchment.save(spec)

    return catchment


def edit_catchment(spec: str):
    catchment = Catchment.load(spec)

    # Example edit: change the duck for order_weekly pond
    duck_motherduck = duck_motherduck()
    catchment.set_pond_ducks({
        "order_weekly": "big",
    }, overwrite=True)

    # Save the catchment
    catchment.save(spec)

    return catchment