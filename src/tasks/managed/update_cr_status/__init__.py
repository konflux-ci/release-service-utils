"""Update the passed CR status with the contents stored in the files in the results dir."""

from . import update_cr_status  # noqa: F401
from .update_cr_status import (  # noqa: F401
    main,
    merge_results_dir,
    patch_status,
    run,
)
