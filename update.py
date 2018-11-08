from replay import Patch_7_07

import database

currentTime = Patch_7_07

# Get our session
session = database.getSession()

#database.updateReplays(Patch_7_07, session)
database.updateAmateurHeroes()