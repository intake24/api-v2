package uk.ac.ncl.openlab.intake24.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import uk.ac.ncl.openlab.intake24.dbutils.DatabaseClient
import uk.ac.ncl.openlab.intake24.foodsql.Tables


@Singleton
class PortionSizeMethodsService @Inject() constructor(@Named("foods") private val foodDatabase: DatabaseClient) {

    private val logger = LoggerFactory.getLogger(PortionSizeMethodsService::class.java)

    fun getAsServedSetIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(Tables.AS_SERVED_SETS.ID).from(Tables.AS_SERVED_SETS).fetchSet(Tables.AS_SERVED_SETS.ID)
        }
    }

    fun getGuideImageIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(Tables.GUIDE_IMAGES.ID).from(Tables.GUIDE_IMAGES).fetchSet(Tables.GUIDE_IMAGES.ID)
        }
    }

    fun getDrinkwareIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(Tables.DRINKWARE_SETS.ID).from(Tables.DRINKWARE_SETS).fetchSet(Tables.DRINKWARE_SETS.ID)
        }
    }
}
