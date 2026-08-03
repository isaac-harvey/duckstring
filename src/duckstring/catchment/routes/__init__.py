from fastapi import APIRouter

from .alerts import router as alerts_router
from .catchment import router as catchment_router
from .data import router as data_router
from .deploy import router as deploy_router
from .draw import router as draw_router
from .duck import router as duck_router
from .duct import router as duct_router
from .orchestrate import router as orchestrate_router
from .pool import router as pool_router
from .secrets import router as secrets_router
from .serving import router as serving_router
from .view import router as view_router

router = APIRouter()
router.include_router(catchment_router)
router.include_router(deploy_router)
router.include_router(data_router)
router.include_router(orchestrate_router)
router.include_router(duck_router)
router.include_router(pool_router)
router.include_router(draw_router)
router.include_router(duct_router)
router.include_router(view_router)
router.include_router(secrets_router)
router.include_router(serving_router)
router.include_router(alerts_router)

__all__ = ["router"]
