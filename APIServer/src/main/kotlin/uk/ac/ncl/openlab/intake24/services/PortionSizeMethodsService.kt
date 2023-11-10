package uk.ac.ncl.openlab.intake24.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import uk.ac.ncl.openlab.intake24.dbutils.DatabaseClient
import uk.ac.ncl.openlab.intake24.foodsql.Tables.*;

data class ImageMapObject(val id: Int, val description: String, val navigationIndex: Int, val outlineCoordinates: List<Double>)

data class PortableImageMap(val description: String, val baseImagePath: String, val objects: List<ImageMapObject>)

data class PortableDrinkScale(
    val width: Int, val height: Int, val emptyLevel: Int, val fullLevel: Int,
    val baseImagePath: String, val overlayImagePath: String, val volumeSamples: List<Double>
)

data class PortableDrinkwareSet(val description: String, val selectionImageMapId: String, val scales: Map<Int, PortableDrinkScale>)


@Singleton
class PortionSizeMethodsService @Inject() constructor(@Named("foods") private val foodDatabase: DatabaseClient) {

    private data class DrinkScaleRow(
        val id: Int,
        val choiceId: Int,
        val width: Int, val height: Int, val emptyLevel: Int, val fullLevel: Int,
        val baseImagePath: String, val overlayImagePath: String
    )


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

            val objects = it.select(
                IMAGE_MAP_OBJECTS.ID,
                IMAGE_MAP_OBJECTS.DESCRIPTION,
                IMAGE_MAP_OBJECTS.NAVIGATION_INDEX,
                IMAGE_MAP_OBJECTS.OUTLINE_COORDINATES
            )
                .from(IMAGE_MAP_OBJECTS)
                .where(IMAGE_MAP_OBJECTS.IMAGE_MAP_ID.eq(imageMapId))
                .orderBy(IMAGE_MAP_OBJECTS.ID)
                .fetch {
                    ImageMapObject(
                        it[IMAGE_MAP_OBJECTS.ID],
                        it[IMAGE_MAP_OBJECTS.DESCRIPTION],
                        it[IMAGE_MAP_OBJECTS.NAVIGATION_INDEX],
                        it[IMAGE_MAP_OBJECTS.OUTLINE_COORDINATES].toList()
                    )
                }

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

    fun exportDrinkwareSet(drinkwareSetId: String): PortableDrinkwareSet? {
        return foodDatabase.runTransaction {

            val scaleRows = it.select(
                DRINKWARE_SCALES.ID,
                DRINKWARE_SCALES.CHOICE_ID,
                DRINKWARE_SCALES.WIDTH,
                DRINKWARE_SCALES.HEIGHT,
                DRINKWARE_SCALES.EMPTY_LEVEL,
                DRINKWARE_SCALES.FULL_LEVEL,
                DRINKWARE_SCALES.BASE_IMAGE_URL,
                DRINKWARE_SCALES.OVERLAY_IMAGE_URL
            ).from(DRINKWARE_SCALES)
                .where(DRINKWARE_SCALES.DRINKWARE_SET_ID.eq(drinkwareSetId))
                .fetch {
                    DrinkScaleRow(
                        it[DRINKWARE_SCALES.ID],
                        it[DRINKWARE_SCALES.CHOICE_ID],
                        it[DRINKWARE_SCALES.WIDTH],
                        it[DRINKWARE_SCALES.HEIGHT],
                        it[DRINKWARE_SCALES.EMPTY_LEVEL],
                        it[DRINKWARE_SCALES.FULL_LEVEL],
                        it[DRINKWARE_SCALES.BASE_IMAGE_URL],
                        it[DRINKWARE_SCALES.OVERLAY_IMAGE_URL]
                    )

                }

            val scaleIds = scaleRows.map { it.id };

            val volumeSamplesMap =
                it.select(DRINKWARE_VOLUME_SAMPLES.DRINKWARE_SCALE_ID, DRINKWARE_VOLUME_SAMPLES.FILL, DRINKWARE_VOLUME_SAMPLES.VOLUME)
                    .from(DRINKWARE_VOLUME_SAMPLES)
                    .where(DRINKWARE_VOLUME_SAMPLES.DRINKWARE_SCALE_ID.`in`(scaleIds))
                    .fetchGroups(DRINKWARE_VOLUME_SAMPLES.DRINKWARE_SCALE_ID)

            it.select(DRINKWARE_SETS.DESCRIPTION, DRINKWARE_SETS.GUIDE_IMAGE_ID)
                .from(DRINKWARE_SETS)
                .where(DRINKWARE_SETS.ID.eq(drinkwareSetId))
                .fetchOne { setRow ->

                    val scales = scaleRows.map { scaleRow ->
                        val volumeSamples =
                            volumeSamplesMap[scaleRow.id]!!.flatMap { sampleRow ->
                                listOf(sampleRow[DRINKWARE_VOLUME_SAMPLES.FILL], sampleRow[DRINKWARE_VOLUME_SAMPLES.VOLUME])
                            }

                        Pair(
                            scaleRow.choiceId,
                            PortableDrinkScale(
                                scaleRow.width,
                                scaleRow.height,
                                scaleRow.emptyLevel,
                                scaleRow.fullLevel,
                                scaleRow.baseImagePath,
                                scaleRow.overlayImagePath,
                                volumeSamples
                            )
                        )
                    }.toMap()

                    PortableDrinkwareSet(setRow[DRINKWARE_SETS.DESCRIPTION], setRow[DRINKWARE_SETS.GUIDE_IMAGE_ID], scales)
                }
        }
    }
}
