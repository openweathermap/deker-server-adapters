"""The bounds of the adapter tests: the index expressions of DEKER and the ones that travel as strings.

``index_exp_params.py`` is a copy of ``tests/parameters/index_exp_params.py`` of the deker repository; the copies are
kept equal by hand. The array of the tests is 10x10x10: a plain axis, an axis with the labels a..j and a time axis
with the step of three days, as on the server.
"""

from datetime import datetime, timedelta, timezone

import numpy as np

from tests.params.index_exp_params import STEPPED_INDEX_EXPS, as_params, invalid_index_exps, valid_index_exps

# what travels as strings only: labels, datetimes and timedeltas; the subset turns them into positions
fancy_index_params = [
    np.index_exp[:, :, datetime.fromtimestamp(1663200000, tz=timezone.utc)],
    np.index_exp[:, :, 1663200000.0],
    np.index_exp[:, 0, datetime.fromtimestamp(1663200000, tz=timezone.utc).isoformat()],
    np.index_exp[:, "a":"j":3],
    np.index_exp[:, "j":"a":-2, :: -timedelta(days=9)],
    np.index_exp[:, :, :: timedelta(days=9)],
    np.index_exp[:, :, :: -timedelta(days=6)],
    np.index_exp[:, :, :: -timedelta(days=9)],
]

# the steps of DEKER for one to three axes (the four-axis ones do not fit a 10x10x10 array)
stepped_index_params = STEPPED_INDEX_EXPS[1] + STEPPED_INDEX_EXPS[2] + STEPPED_INDEX_EXPS[3]

valid_index_params = as_params(valid_index_exps + stepped_index_params + fancy_index_params)

# what DEKER refuses before any request is sent
invalid_index_params = as_params(invalid_index_exps)
