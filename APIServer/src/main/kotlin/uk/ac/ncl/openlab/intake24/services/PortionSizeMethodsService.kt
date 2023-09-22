package uk.ac.ncl.openlab.intake24.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import uk.ac.ncl.openlab.intake24.dbutils.DatabaseClient
import uk.ac.ncl.openlab.intake24.foodsql.Tables.*;

data class ImageMapObject(val id: Int, val description: String, val outlineCoordinates: List<Double>)

data class PortableImageMap(val description: String, val baseImagePath: String, val objects: List<ImageMapObject>)

@Singleton
class PortionSizeMethodsService @Inject() constructor(@Named("foods") private val foodDatabase: DatabaseClient) {

    private val logger = LoggerFactory.getLogger(PortionSizeMethodsService::class.java)

    fun getAsServedSetIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(AS_SERVED_SETS.ID).from(AS_SERVED_SETS).fetchSet(AS_SERVED_SETS.ID)
        }
    }

    fun getGuideImageIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(GUIDE_IMAGES.ID).from(GUIDE_IMAGES).fetchSet(GUIDE_IMAGES.ID)
        }
    }

    fun getDrinkwareIds(): Set<String> {
        return foodDatabase.runTransaction {
            it.select(DRINKWARE_SETS.ID).from(DRINKWARE_SETS).fetchSet(DRINKWARE_SETS.ID)
        }
    }
    
    fun exportImageMap(imageMapId: String): PortableImageMap? {
        return foodDatabase.runTransaction {

            val objects = it.select(IMAGE_MAP_OBJECTS.ID, IMAGE_MAP_OBJECTS.DESCRIPTION, IMAGE_MAP_OBJECTS.OUTLINE_COORDINATES)
                .from(IMAGE_MAP_OBJECTS)
                .where(IMAGE_MAP_OBJECTS.IMAGE_MAP_ID.eq(imageMapId))
                .orderBy(IMAGE_MAP_OBJECTS.ID)
                .fetch {
                    ImageMapObject(it[IMAGE_MAP_OBJECTS.ID], it[IMAGE_MAP_OBJECTS.DESCRIPTION], it[IMAGE_MAP_OBJECTS.OUTLINE_COORDINATES].toList())
                }

            logger.debug("Image map " + imageMapId + " objects: " + objects.size)

            it.select(IMAGE_MAPS.DESCRIPTION, SOURCE_IMAGES.PATH)
                .from(IMAGE_MAPS)
                .join(PROCESSED_IMAGES).on(PROCESSED_IMAGES.ID.eq(IMAGE_MAPS.BASE_IMAGE_ID))
                .join(SOURCE_IMAGES).on(PROCESSED_IMAGES.SOURCE_ID.eq(SOURCE_IMAGES.ID))
                .where(IMAGE_MAPS.ID.eq(imageMapId))
                .fetchOne {
                    PortableImageMap(it[IMAGE_MAPS.DESCRIPTION], it[SOURCE_IMAGES.PATH], objects)
                }
        }
    }
}
