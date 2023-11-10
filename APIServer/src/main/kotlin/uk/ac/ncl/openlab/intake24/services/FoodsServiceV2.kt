package uk.ac.ncl.openlab.intake24.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import org.apache.commons.lang3.StringUtils
import org.jooq.DSLContext
import org.jooq.impl.DSL.*
import org.slf4j.LoggerFactory
import uk.ac.ncl.openlab.intake24.dbutils.DatabaseClient
import uk.ac.ncl.openlab.intake24.foodsql.Routines
import uk.ac.ncl.openlab.intake24.foodsql.Tables.*
import uk.ac.ncl.openlab.intake24.tools.FoodCompositionTableReference
import java.util.*


data class InheritableAttributes(
    val readyMealOption: Boolean?, val sameAsBeforeOption: Boolean?, val reasonableAmount: Int?,
    val useInRecipes: Int?
)

data class NewFoodV2(
    val code: String, val englishDescription: String, val groupCode: Int, val attributes: InheritableAttributes,
    val categories: List<String>
)

data class NewLocalFoodV2(
    val code: String, val localDescription: String?, val nutrientTableCodes: List<FoodCompositionTableReference>,
    val portionSize: List<PortionSizeMethod>, val associatedFoods: List<AssociatedFood>, val brandNames: List<String>
)

data class CopyFoodV2(val sourceCode: String, val newCode: String, val newDescription: String)

data class CopyLocalV2(
    val sourceCode: String,
    val destCode: String,
    val localDescription: String,
    val nutrientTableCodes: List<FoodCompositionTableReference>
)


data class PortionSizeMethodParameter(val name: String, val value: String)


sealed class FoodOrCategoryCode

data class FoodCode(val code: String) : FoodOrCategoryCode()

data class CategoryCode(val code: String) : FoodOrCategoryCode()

data class AssociatedFood(val foodOrCategoryCode: FoodOrCategoryCode, val promptText: String, val linkAsMain: Boolean, val genericName: String)

data class LocalFoodsListRow(val code: String, val englishDescription: String, val localDescription: String?)

data class CategoryLocalRow(val categoryCode: String, val localeId: String, val localDescription: String, val simpleLocalDescription: String)

data class PortionSizeMethod(
    val method: String, val description: String, val imageUrl: String, val useForRecipes: Boolean,
    val conversionFactor: Double, val parameters: List<PortionSizeMethodParameter>
) {
    companion object {

        fun guideImage(
            guideImageId: String,
            description: String = "use_an_image",
            userForRecipes: Boolean = true,
            conversionFactor: Double = 1.0
        ): PortionSizeMethod {
            return PortionSizeMethod(
                "guide-image", description, "standard-portion.jpg", userForRecipes, conversionFactor,
                listOf(PortionSizeMethodParameter("guide-image-id", guideImageId))
            )
        }

        fun asServed(
            servingSetId: String,
            leftoversSetId: String? = null,
            description: String = "use_an_image",
            userForRecipes: Boolean = true,
            conversionFactor: Double = 1.0
        ): PortionSizeMethod {

            val params =
                listOfNotNull(
                    PortionSizeMethodParameter("serving-image-set", servingSetId),
                    leftoversSetId?.let { PortionSizeMethodParameter("leftovers-image-set", it) })

            return PortionSizeMethod("as-served", description, "standard-portion.jpg", userForRecipes, conversionFactor, params)
        }

        fun drinkScale(
            drinkScaleId: String,
            description: String = "use_an_image",
            initialFillLevel: Double = 0.9,
            skipFillLevel: Boolean = false,
            useForRecipes: Boolean = true,
            conversionFactor: Double = 1.0
        ): PortionSizeMethod {

            val params = listOf(
                PortionSizeMethodParameter("drinkware-id", drinkScaleId),
                PortionSizeMethodParameter("initial-fill-level", initialFillLevel.toString()),
                PortionSizeMethodParameter("skip-fill-level", skipFillLevel.toString())
            )

            return PortionSizeMethod("drink-scale", description, "standard-portion.jpg", useForRecipes, conversionFactor, params)
        }

        fun cereal(
            cerealType: String,
            description: String = "use_an_image",
            useForRecipes: Boolean = true,
            conversionFactor: Double = 1.0
        ): PortionSizeMethod {

            val params = listOf(PortionSizeMethodParameter("type", cerealType))

            return PortionSizeMethod("cereal", description, "standard-portion.jpg", useForRecipes, conversionFactor, params)

        }

        fun pizza(description: String = "use_an_image", useForRecipes: Boolean = true, conversionFactor: Double = 1.0): PortionSizeMethod {
            return PortionSizeMethod("pizza", description, "standard-portion.jpg", useForRecipes, conversionFactor, emptyList())
        }

    }
}


data class UpdateLocalFoodV2(
    val code: String, val baseVersion: UUID?, val newCode: String, val localDescription: String?,
    val nutrientTableCodes: List<FoodCompositionTableReference>, val portionSize: List<PortionSizeMethod>,
    val associatedFoods: List<AssociatedFood>, val brandNames: List<String>
)


data class CategoryHeader(val code: String, val englishDescription: String, val localDescription: List<String>, val isHidden: Boolean)


data class FoodHeader(val code: String, val englishDescription: String, val localDescription: List<String>)


data class CategoryContents(val foods: List<FoodHeader>, val subcategories: List<CategoryHeader>)


@Singleton
class FoodsServiceV2 @Inject() constructor(@Named("foods") private val foodDatabase: DatabaseClient) {

    companion object {
        const val USE_ANYWHERE = 0
        const val USE_AS_REGULAR_FOOD = 1
        const val USE_AS_RECIPE_INGREDIENT = 2

        private fun <T> assertNonEmpty(list: List<T>) {
            if (list.isEmpty())
                throw RuntimeException("This function must not be called with an empty list")
        }


        fun getPortionSizeMethods(foodCodes: List<String>, localeId: String, context: DSLContext): Map<String, List<PortionSizeMethod>> {
            if (foodCodes.isNotEmpty()) {

                val portionSizeMethodRows = context.select(
                    FOODS_PORTION_SIZE_METHODS.ID,
                    FOODS_PORTION_SIZE_METHODS.FOOD_CODE,
                    FOODS_PORTION_SIZE_METHODS.LOCALE_ID,
                    FOODS_PORTION_SIZE_METHODS.METHOD,
                    FOODS_PORTION_SIZE_METHODS.DESCRIPTION,
                    FOODS_PORTION_SIZE_METHODS.IMAGE_URL,
                    FOODS_PORTION_SIZE_METHODS.USE_FOR_RECIPES,
                    FOODS_PORTION_SIZE_METHODS.CONVERSION_FACTOR
                )
                    .from(FOODS_PORTION_SIZE_METHODS)
                    .where(FOODS_PORTION_SIZE_METHODS.FOOD_CODE.`in`(foodCodes).and(FOODS_PORTION_SIZE_METHODS.LOCALE_ID.eq(localeId)))
                    .orderBy(FOODS_PORTION_SIZE_METHODS.ID)
                    .fetchArray()

                val ids = portionSizeMethodRows.map { it[FOODS_PORTION_SIZE_METHODS.ID] }

                val parameters = context.select(
                    FOODS_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID,
                    FOODS_PORTION_SIZE_METHOD_PARAMS.NAME,
                    FOODS_PORTION_SIZE_METHOD_PARAMS.VALUE
                )
                    .from(FOODS_PORTION_SIZE_METHOD_PARAMS)
                    .where(FOODS_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID.`in`(ids))
                    .orderBy(FOODS_PORTION_SIZE_METHOD_PARAMS.ID)
                    .fold(emptyMap<Int, List<PortionSizeMethodParameter>>()) { map, row ->
                        val id = row[FOODS_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID]
                        val list = map[id] ?: emptyList()

                        map + Pair(
                            id,
                            list + PortionSizeMethodParameter(row[FOODS_PORTION_SIZE_METHOD_PARAMS.NAME], row[FOODS_PORTION_SIZE_METHOD_PARAMS.VALUE])
                        )
                    }


                val empty = foodCodes.fold(emptyMap<String, List<PortionSizeMethod>>()) { map, code ->
                    map + Pair(code, emptyList())
                }

                return portionSizeMethodRows.fold(empty) { map, row ->

                    val foodCode = row[FOODS_PORTION_SIZE_METHODS.FOOD_CODE]
                    val methods = map[foodCode] ?: emptyList()

                    map + Pair(
                        foodCode, methods + PortionSizeMethod(
                            row[FOODS_PORTION_SIZE_METHODS.METHOD],
                            row[FOODS_PORTION_SIZE_METHODS.DESCRIPTION],
                            row[FOODS_PORTION_SIZE_METHODS.IMAGE_URL],
                            row[FOODS_PORTION_SIZE_METHODS.USE_FOR_RECIPES],
                            row[FOODS_PORTION_SIZE_METHODS.CONVERSION_FACTOR],
                            parameters[row[FOODS_PORTION_SIZE_METHODS.ID]] ?: emptyList()
                        )

                    )
                }

            } else
                return emptyMap()
        }

        fun getCategoryPortionSizeMethods(categoryCodes: List<String>, localeId: String, context: DSLContext): Map<String, List<PortionSizeMethod>> {
            if (categoryCodes.isNotEmpty()) {

                val portionSizeMethodRows = context.select(
                    CATEGORIES_PORTION_SIZE_METHODS.ID,
                    CATEGORIES_PORTION_SIZE_METHODS.CATEGORY_CODE,
                    CATEGORIES_PORTION_SIZE_METHODS.LOCALE_ID,
                    CATEGORIES_PORTION_SIZE_METHODS.METHOD,
                    CATEGORIES_PORTION_SIZE_METHODS.DESCRIPTION,
                    CATEGORIES_PORTION_SIZE_METHODS.IMAGE_URL,
                    CATEGORIES_PORTION_SIZE_METHODS.USE_FOR_RECIPES,
                    CATEGORIES_PORTION_SIZE_METHODS.CONVERSION_FACTOR
                )
                    .from(CATEGORIES_PORTION_SIZE_METHODS)
                    .where(
                        CATEGORIES_PORTION_SIZE_METHODS.CATEGORY_CODE.`in`(categoryCodes).and(CATEGORIES_PORTION_SIZE_METHODS.LOCALE_ID.eq(localeId))
                    )
                    .orderBy(CATEGORIES_PORTION_SIZE_METHODS.ID)
                    .fetchArray()

                val ids = portionSizeMethodRows.map { it[CATEGORIES_PORTION_SIZE_METHODS.ID] }

                val parameters = context.select(
                    CATEGORIES_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID,
                    CATEGORIES_PORTION_SIZE_METHOD_PARAMS.NAME,
                    CATEGORIES_PORTION_SIZE_METHOD_PARAMS.VALUE
                )
                    .from(CATEGORIES_PORTION_SIZE_METHOD_PARAMS)
                    .where(CATEGORIES_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID.`in`(ids))
                    .orderBy(CATEGORIES_PORTION_SIZE_METHOD_PARAMS.ID)
                    .fold(emptyMap<Int, List<PortionSizeMethodParameter>>()) { map, row ->
                        val id = row[CATEGORIES_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID]
                        val list = map[id] ?: emptyList()

                        map + Pair(
                            id,
                            list + PortionSizeMethodParameter(
                                row[CATEGORIES_PORTION_SIZE_METHOD_PARAMS.NAME],
                                row[CATEGORIES_PORTION_SIZE_METHOD_PARAMS.VALUE]
                            )
                        )
                    }


                val empty = categoryCodes.fold(emptyMap<String, List<PortionSizeMethod>>()) { map, code ->
                    map + Pair(code, emptyList())
                }

                return portionSizeMethodRows.fold(empty) { map, row ->

                    val categoryCode = row[CATEGORIES_PORTION_SIZE_METHODS.CATEGORY_CODE]
                    val methods = map[categoryCode] ?: emptyList()

                    map + Pair(
                        categoryCode, methods + PortionSizeMethod(
                            row[CATEGORIES_PORTION_SIZE_METHODS.METHOD],
                            row[CATEGORIES_PORTION_SIZE_METHODS.DESCRIPTION],
                            row[CATEGORIES_PORTION_SIZE_METHODS.IMAGE_URL],
                            row[CATEGORIES_PORTION_SIZE_METHODS.USE_FOR_RECIPES],
                            row[CATEGORIES_PORTION_SIZE_METHODS.CONVERSION_FACTOR],
                            parameters[row[CATEGORIES_PORTION_SIZE_METHODS.ID]] ?: emptyList()
                        )

                    )
                }

            } else
                return emptyMap()
        }


        fun updatePortionSizeMethods(updates: List<Pair<String, List<PortionSizeMethod>>>, localeId: String, context: DSLContext) {
            if (updates.isNotEmpty()) {
                context.deleteFrom(FOODS_PORTION_SIZE_METHODS)
                    .where(FOODS_PORTION_SIZE_METHODS.FOOD_CODE.`in`(updates.map { it.first }).and(FOODS_PORTION_SIZE_METHODS.LOCALE_ID.eq(localeId)))
                    .execute()

                if (updates.any { it.second.isNotEmpty() }) {
                    val insertQuery1 = context.insertInto(
                        FOODS_PORTION_SIZE_METHODS,
                        FOODS_PORTION_SIZE_METHODS.FOOD_CODE,
                        FOODS_PORTION_SIZE_METHODS.LOCALE_ID,
                        FOODS_PORTION_SIZE_METHODS.METHOD,
                        FOODS_PORTION_SIZE_METHODS.DESCRIPTION,
                        FOODS_PORTION_SIZE_METHODS.IMAGE_URL,
                        FOODS_PORTION_SIZE_METHODS.USE_FOR_RECIPES,
                        FOODS_PORTION_SIZE_METHODS.CONVERSION_FACTOR
                    )

                    val methodCodes = updates.fold(insertQuery1) { query, update ->
                        val foodCode = update.first
                        val methods = update.second

                        methods.fold(query) { query, method ->
                            query.values(
                                foodCode, localeId, method.method, method.description, method.imageUrl,
                                method.useForRecipes, method.conversionFactor
                            )
                        }
                    }.returningResult(FOODS_PORTION_SIZE_METHODS.ID).fetch().intoArray(FOODS_PORTION_SIZE_METHODS.ID)


                    val paramUpdates = methodCodes.zip(updates.flatMap { it.second.map { it.parameters } })

                    if (paramUpdates.any { it.second.isNotEmpty() }) {
                        val paramsInsertQuery1 = context.insertInto(
                            FOODS_PORTION_SIZE_METHOD_PARAMS,
                            FOODS_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID,
                            FOODS_PORTION_SIZE_METHOD_PARAMS.NAME,
                            FOODS_PORTION_SIZE_METHOD_PARAMS.VALUE
                        )

                        paramUpdates.fold(paramsInsertQuery1) { query, update ->

                            val methodId = update.first
                            val parameters = update.second

                            parameters.fold(query) { query, parameter ->
                                query.values(methodId, parameter.name, parameter.value)
                            }
                        }.execute()
                    }
                }
            }
        }

        fun updateCategoryPortionSizeMethods(updates: List<Pair<String, List<PortionSizeMethod>>>, localeId: String, context: DSLContext) {
            if (updates.isNotEmpty()) {
                context.deleteFrom(CATEGORIES_PORTION_SIZE_METHODS)
                    .where(
                        CATEGORIES_PORTION_SIZE_METHODS.CATEGORY_CODE.`in`(updates.map { it.first })
                            .and(CATEGORIES_PORTION_SIZE_METHODS.LOCALE_ID.eq(localeId))
                    )
                    .execute()

                if (updates.any { it.second.isNotEmpty() }) {
                    val insertQuery1 = context.insertInto(
                        CATEGORIES_PORTION_SIZE_METHODS,
                        CATEGORIES_PORTION_SIZE_METHODS.CATEGORY_CODE,
                        CATEGORIES_PORTION_SIZE_METHODS.LOCALE_ID,
                        CATEGORIES_PORTION_SIZE_METHODS.METHOD,
                        CATEGORIES_PORTION_SIZE_METHODS.DESCRIPTION,
                        CATEGORIES_PORTION_SIZE_METHODS.IMAGE_URL,
                        CATEGORIES_PORTION_SIZE_METHODS.USE_FOR_RECIPES,
                        CATEGORIES_PORTION_SIZE_METHODS.CONVERSION_FACTOR
                    )

                    val methodCodes = updates.fold(insertQuery1) { query, update ->
                        val categoryCode = update.first
                        val methods = update.second

                        methods.fold(query) { query, method ->
                            query.values(
                                categoryCode, localeId, method.method, method.description, method.imageUrl,
                                method.useForRecipes, method.conversionFactor
                            )
                        }
                    }.returningResult(CATEGORIES_PORTION_SIZE_METHODS.ID).fetch().intoArray(CATEGORIES_PORTION_SIZE_METHODS.ID)


                    val paramUpdates = methodCodes.zip(updates.flatMap { it.second.map { it.parameters } })

                    if (paramUpdates.any { it.second.isNotEmpty() }) {
                        val paramsInsertQuery1 = context.insertInto(
                            CATEGORIES_PORTION_SIZE_METHOD_PARAMS,
                            CATEGORIES_PORTION_SIZE_METHOD_PARAMS.PORTION_SIZE_METHOD_ID,
                            CATEGORIES_PORTION_SIZE_METHOD_PARAMS.NAME,
                            CATEGORIES_PORTION_SIZE_METHOD_PARAMS.VALUE
                        )

                        paramUpdates.fold(paramsInsertQuery1) { query, update ->

                            val methodId = update.first
                            val parameters = update.second

                            parameters.fold(query) { query, parameter ->
                                query.values(methodId, parameter.name, parameter.value)
                            }
                        }.execute()
                    }
                }
            }
        }


        fun getNutrientTableCodes(foodCodes: List<String>, localeId: String, context: DSLContext): Map<String, List<FoodCompositionTableReference>> {
            if (foodCodes.isNotEmpty()) {

                val empty = foodCodes.fold(emptyMap<String, List<FoodCompositionTableReference>>()) { map, code ->
                    map + Pair(code, emptyList())
                }


                return context.select(
                    FOODS_NUTRIENT_MAPPING.FOOD_CODE,
                    FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_ID,
                    FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_RECORD_ID
                )
                    .from(FOODS_NUTRIENT_MAPPING)
                    .where(FOODS_NUTRIENT_MAPPING.FOOD_CODE.`in`(foodCodes).and(FOODS_NUTRIENT_MAPPING.LOCALE_ID.eq(localeId)))
                    .fetchArray()
                    .fold(empty) { map, row ->
                        val code = row[FOODS_NUTRIENT_MAPPING.FOOD_CODE]
                        val list = map[code] ?: emptyList()

                        map + Pair(
                            code, list + FoodCompositionTableReference(
                                row[FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_ID],
                                row[FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_RECORD_ID]
                            )
                        )
                    }
            } else
                return emptyMap()
        }

        fun updateNutrientTableCodes(updates: List<Pair<String, List<FoodCompositionTableReference>>>, localeId: String, context: DSLContext) {
            if (updates.isNotEmpty()) {
                val deleteQuery = context.deleteFrom(FOODS_NUTRIENT_MAPPING)
                    .where(FOODS_NUTRIENT_MAPPING.FOOD_CODE.`in`(updates.map { it.first }).and(FOODS_NUTRIENT_MAPPING.LOCALE_ID.eq(localeId)))


                if (updates.any { it.second.isNotEmpty() }) {

                    val insertQuery1 = context.insertInto(
                        FOODS_NUTRIENT_MAPPING,
                        FOODS_NUTRIENT_MAPPING.FOOD_CODE,
                        FOODS_NUTRIENT_MAPPING.LOCALE_ID,
                        FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_ID,
                        FOODS_NUTRIENT_MAPPING.NUTRIENT_TABLE_RECORD_ID
                    )


                    val insertQuery = updates.fold(insertQuery1) { query, update ->
                        val foodCode = update.first
                        val codes = update.second

                        codes.fold(query) { query, fctRef ->
                            query.values(foodCode, localeId, fctRef.tableId, fctRef.recordId)
                        }
                    }

                    context.batch(deleteQuery, insertQuery).execute()
                } else
                    deleteQuery.execute()
            }
        }

        fun getBrands(foodCodes: List<String>, localeId: String, context: DSLContext): Map<String, List<String>> {
            if (foodCodes.isNotEmpty()) {

                val empty = foodCodes.fold(emptyMap<String, List<String>>()) { map, code ->
                    map + Pair(code, emptyList())
                }

                return context.select(BRANDS.FOOD_CODE, BRANDS.NAME)
                    .from(BRANDS)
                    .where(BRANDS.FOOD_CODE.`in`(foodCodes).and(BRANDS.LOCALE_ID.eq(localeId)))
                    .orderBy(BRANDS.ID)
                    .fetchArray()
                    .fold(empty) { map, row ->
                        val code = row[BRANDS.FOOD_CODE]
                        val list = map[code] ?: emptyList()
                        map + Pair(code, list + row[BRANDS.NAME])
                    }
            } else
                return emptyMap()
        }

        fun updateBrands(updates: List<Pair<String, List<String>>>, localeId: String, context: DSLContext) {
            if (updates.isNotEmpty()) {

                val deleteQuery = context.deleteFrom(BRANDS)
                    .where(BRANDS.FOOD_CODE.`in`(updates.map { it.first }).and(BRANDS.LOCALE_ID.eq(localeId)))

                if (updates.any { it.second.isNotEmpty() }) {

                    val insertQuery1 = context.insertInto(
                        BRANDS,
                        BRANDS.FOOD_CODE,
                        BRANDS.LOCALE_ID,
                        BRANDS.NAME
                    )


                    val insertQuery = updates.fold(insertQuery1) { query, update ->
                        val foodCode = update.first
                        val brands = update.second

                        brands.fold(query) { query, brand ->
                            query.values(foodCode, localeId, brand)
                        }
                    }

                    context.batch(deleteQuery, insertQuery).execute()
                } else
                    deleteQuery.execute()
            }
        }

        fun getAssociatedFoods(foodCodes: List<String>, localeId: String, context: DSLContext): Map<String, List<AssociatedFood>> {
            if (foodCodes.isNotEmpty()) {

                val empty = foodCodes.fold(emptyMap<String, List<AssociatedFood>>()) { map, code ->
                    map + Pair(code, emptyList())
                }

                return context.select(
                    ASSOCIATED_FOODS.FOOD_CODE,
                    ASSOCIATED_FOODS.LOCALE_ID,
                    ASSOCIATED_FOODS.ASSOCIATED_FOOD_CODE,
                    ASSOCIATED_FOODS.ASSOCIATED_CATEGORY_CODE,
                    ASSOCIATED_FOODS.TEXT,
                    ASSOCIATED_FOODS.LINK_AS_MAIN,
                    ASSOCIATED_FOODS.GENERIC_NAME
                )
                    .from(ASSOCIATED_FOODS)
                    .where(ASSOCIATED_FOODS.FOOD_CODE.`in`(foodCodes).and(ASSOCIATED_FOODS.LOCALE_ID.eq(localeId)))
                    .orderBy(ASSOCIATED_FOODS.ID)
                    .fetchArray()
                    .fold(empty) { map, row ->
                        val code = row[ASSOCIATED_FOODS.FOOD_CODE]
                        val list = map[code] ?: emptyList()

                        val assocFoodCode = row[ASSOCIATED_FOODS.ASSOCIATED_FOOD_CODE]
                        val assocCategoryCode = row[ASSOCIATED_FOODS.ASSOCIATED_CATEGORY_CODE]

                        val foodOrCategoryCode = if (assocFoodCode != null)
                            FoodCode(assocFoodCode)
                        else
                            CategoryCode(assocCategoryCode)

                        map + Pair(
                            code, list + AssociatedFood(
                                foodOrCategoryCode, row[ASSOCIATED_FOODS.TEXT],
                                row[ASSOCIATED_FOODS.LINK_AS_MAIN], row[ASSOCIATED_FOODS.GENERIC_NAME]
                            )
                        )
                    }
            } else
                return emptyMap()
        }

        fun updateAssociatedFoods(updates: List<Pair<String, List<AssociatedFood>>>, localeId: String, context: DSLContext) {
            if (updates.isNotEmpty()) {

                val deleteQuery = context.deleteFrom(ASSOCIATED_FOODS)
                    .where(ASSOCIATED_FOODS.FOOD_CODE.`in`(updates.map { it.first }).and(ASSOCIATED_FOODS.LOCALE_ID.eq(localeId)))

                if (updates.any { it.second.isNotEmpty() }) {
                    val insertQuery1 = context.insertInto(
                        ASSOCIATED_FOODS,
                        ASSOCIATED_FOODS.FOOD_CODE,
                        ASSOCIATED_FOODS.LOCALE_ID,
                        ASSOCIATED_FOODS.ASSOCIATED_FOOD_CODE,
                        ASSOCIATED_FOODS.ASSOCIATED_CATEGORY_CODE,
                        ASSOCIATED_FOODS.TEXT,
                        ASSOCIATED_FOODS.LINK_AS_MAIN,
                        ASSOCIATED_FOODS.GENERIC_NAME
                    )

                    val insertQuery = updates.fold(insertQuery1) { query, update ->
                        val foodCode = update.first
                        val associatedFoods = update.second

                        associatedFoods.fold(query) { query, associatedFood ->

                            val associatedFoodCode = when (associatedFood.foodOrCategoryCode) {
                                is FoodCode -> associatedFood.foodOrCategoryCode.code
                                is CategoryCode -> null
                            }

                            val associatedCategoryCode = when (associatedFood.foodOrCategoryCode) {
                                is FoodCode -> null
                                is CategoryCode -> associatedFood.foodOrCategoryCode.code
                            }

                            query.values(
                                foodCode, localeId, associatedFoodCode, associatedCategoryCode, associatedFood.promptText,
                                associatedFood.linkAsMain, associatedFood.genericName
                            )
                        }
                    }

                    context.batch(deleteQuery, insertQuery).execute()
                } else
                    deleteQuery.execute()
            }
        }

        fun validateLocaleId(localeId: String, context: DSLContext) {
            if (context.selectOne().from(LOCALES).where(LOCALES.ID.eq(localeId)).fetchAny() == null)
                throw IllegalArgumentException("Locale $localeId does not exist")
        }
    }

    private val logger = LoggerFactory.getLogger(FoodsServiceV2::class.java)

    private fun truncateDescription(description: String?, foodCode: String): String? {
        if (description == null)
            return null;

        if (description.length > 128) {
            logger.warn("Description too long for food $foodCode, truncating:")
            logger.warn(description)
            return description.take(128)
        } else
            return description
    }

    fun getDuplicateCodes(newCodes: Set<String>): Set<String> {
        return foodDatabase.runTransaction {
            it.select(FOODS.CODE).from(FOODS).where(FOODS.CODE.`in`(newCodes))
                .fetchArray(FOODS.CODE)
        }.toSet()
    }

    fun createFood(food: NewFoodV2) {
        createFoods(listOf(food))
    }

    fun createFoods(foods: List<NewFoodV2>) {
        foodDatabase.runTransaction {
            createFoods(foods, it)
        }
    }

    fun createFoods(foods: List<NewFoodV2>, context: DSLContext) {
        if (foods.isNotEmpty()) {


            logger.debug("Writing ${foods.size} new food records to database")

            foods.sortedBy { it.code }.forEach {

                logger.debug("${it.code} | ${it.englishDescription}")
            }

            val foodsInsertQuery = context.insertInto(FOODS, FOODS.CODE, FOODS.DESCRIPTION, FOODS.FOOD_GROUP_ID, FOODS.VERSION)

            foods.fold(foodsInsertQuery) { query, food ->
                query.values(food.code, truncateDescription(food.englishDescription, food.code), food.groupCode, UUID.randomUUID())
            }.execute()

            val attributesInsertQuery = context.insertInto(
                FOODS_ATTRIBUTES, FOODS_ATTRIBUTES.FOOD_CODE, FOODS_ATTRIBUTES.SAME_AS_BEFORE_OPTION,
                FOODS_ATTRIBUTES.READY_MEAL_OPTION, FOODS_ATTRIBUTES.REASONABLE_AMOUNT, FOODS_ATTRIBUTES.USE_IN_RECIPES
            )

            foods.fold(attributesInsertQuery) { query, food ->
                query.values(
                    food.code, food.attributes.sameAsBeforeOption, food.attributes.readyMealOption,
                    food.attributes.reasonableAmount, food.attributes.useInRecipes
                )

            }.execute()

            val categoriesInsertQuery = context.insertInto(FOODS_CATEGORIES, FOODS_CATEGORIES.FOOD_CODE, FOODS_CATEGORIES.CATEGORY_CODE)

            foods.fold(categoriesInsertQuery) { query, food ->
                food.categories.fold(query) { q2, categoryCode ->
                    q2.values(food.code, categoryCode)
                }
            }.execute()
        } else {
            logger.debug("Empty list")
        }
    }


    fun createLocalFoods(foods: List<NewLocalFoodV2>, localeId: String, context: DSLContext) {
        if (foods.isNotEmpty()) {

            val query = context.insertInto(
                FOODS_LOCAL, FOODS_LOCAL.FOOD_CODE, FOODS_LOCAL.LOCALE_ID,
                FOODS_LOCAL.LOCAL_DESCRIPTION, FOODS_LOCAL.SIMPLE_LOCAL_DESCRIPTION,
                FOODS_LOCAL.VERSION
            )

            foods.fold(query) { insert, food ->
                insert.values(
                    food.code, localeId, truncateDescription(food.localDescription, food.code),
                    truncateDescription(StringUtils.stripAccents(food.localDescription), food.code), UUID.randomUUID()
                )
            }.execute()

            updateNutrientTableCodes(foods.map { Pair(it.code, it.nutrientTableCodes) }, localeId, context)
            updateAssociatedFoods(foods.map { Pair(it.code, it.associatedFoods) }, localeId, context)
            updateBrands(foods.map { Pair(it.code, it.brandNames) }, localeId, context)
            updatePortionSizeMethods(foods.map { Pair(it.code, it.portionSize) }, localeId, context)

        } else {
            logger.debug("Empty list")
        }
    }


    fun addFoodToLocale(foodCode: String, localeId: String) {
        addFoodsToLocale(listOf(foodCode), localeId)
    }

    fun addFoodsToLocale(foodCodes: List<String>, localeId: String) {
        foodDatabase.runTransaction {
            addFoodsToLocale(foodCodes, localeId, it)
        }
    }

    fun addFoodsToLocale(foodCodes: List<String>, localeId: String, context: DSLContext) {
        if (foodCodes.isNotEmpty()) {
            val query = context.insertInto(FOODS_LOCAL_LISTS, FOODS_LOCAL_LISTS.LOCALE_ID, FOODS_LOCAL_LISTS.FOOD_CODE)

            foodCodes.fold(query) { query, foodCode ->
                query.values(localeId, foodCode)
            }.onConflictOnConstraint(FOODS_LOCAL_LISTS.primaryKey).doNothing().execute()
        } else {
            logger.debug("Empty foods list")
        }
    }

    fun removeFoodFromLocale(foodCode: String, localeId: String) {
        removeFoodsFromLocale(listOf(foodCode), localeId)
    }

    fun removeFoodsFromLocale(foodCodes: List<String>, localeId: String) {
        if (foodCodes.isNotEmpty()) {
            foodDatabase.runTransaction {
                it.deleteFrom(FOODS_LOCAL_LISTS)
                    .where(FOODS_LOCAL_LISTS.FOOD_CODE.`in`(foodCodes))
                    .execute()
            }
        } else {
            logger.debug("Empty foods list")
        }
    }

    fun getLocalFoodsList(localeId: String, offset: Int, limit: Int, context: DSLContext): List<LocalFoodsListRow> {
        return context.select(FOODS_LOCAL_LISTS.FOOD_CODE, FOODS.DESCRIPTION, FOODS_LOCAL.LOCAL_DESCRIPTION)
            .from(
                FOODS_LOCAL_LISTS
                    .join(FOODS).on(FOODS_LOCAL_LISTS.FOOD_CODE.eq(FOODS.CODE))
                    .join(FOODS_LOCAL)
                    .on(FOODS_LOCAL_LISTS.FOOD_CODE.eq(FOODS_LOCAL.FOOD_CODE).and(FOODS_LOCAL_LISTS.LOCALE_ID.eq(FOODS_LOCAL.LOCALE_ID)))
            )
            .where(FOODS_LOCAL_LISTS.LOCALE_ID.eq(localeId))
            .orderBy(FOODS_LOCAL_LISTS.FOOD_CODE)
            .offset(offset)
            .limit(limit)
            .fetch { record -> LocalFoodsListRow(record.value1(), record.value2(), record.value3()) }
    }

    fun getLocalFoodsList(localeId: String, offset: Int, limit: Int): List<LocalFoodsListRow> {
        return foodDatabase.runTransaction { getLocalFoodsList(localeId, offset, limit, it) }
    }

    fun copyFoods(foods: List<CopyFoodV2>, context: DSLContext) {
        if (foods.isNotEmpty()) {

            val sourceCodes = foods.map { it.sourceCode }.toSet()

            val existingSources = context
                .select(FOODS.CODE)
                .from(FOODS)
                .where(FOODS.CODE.`in`(sourceCodes))
                .fetchArray(FOODS.CODE)
                .toSet()

            if (existingSources.size != sourceCodes.size) {
                throw IllegalArgumentException("Invalid source food codes: ${sourceCodes.minus(existingSources).joinToString()}")
            }

            val foodsQueries = foods.map {
                context.insertInto(FOODS, FOODS.CODE, FOODS.DESCRIPTION, FOODS.FOOD_GROUP_ID, FOODS.VERSION)
                    .select(
                        context
                            .select(inline(it.newCode), inline(it.newDescription), FOODS.FOOD_GROUP_ID, inline(UUID.randomUUID()))
                            .from(FOODS)
                            .where(FOODS.CODE.eq(it.sourceCode))
                    )
            }

            val foodAttributeQueries = foods.map {
                context.insertInto(
                    FOODS_ATTRIBUTES, FOODS_ATTRIBUTES.FOOD_CODE, FOODS_ATTRIBUTES.SAME_AS_BEFORE_OPTION,
                    FOODS_ATTRIBUTES.READY_MEAL_OPTION, FOODS_ATTRIBUTES.REASONABLE_AMOUNT, FOODS_ATTRIBUTES.USE_IN_RECIPES
                )
                    .select(
                        context.select(
                            inline(it.newCode), FOODS_ATTRIBUTES.SAME_AS_BEFORE_OPTION, FOODS_ATTRIBUTES.READY_MEAL_OPTION,
                            FOODS_ATTRIBUTES.REASONABLE_AMOUNT, FOODS_ATTRIBUTES.USE_IN_RECIPES
                        )
                            .from(FOODS_ATTRIBUTES)
                            .where(FOODS_ATTRIBUTES.FOOD_CODE.eq(it.sourceCode))
                    )
            }

            val foodCategoriesQueries = foods.map {
                context.insertInto(FOODS_CATEGORIES, FOODS_CATEGORIES.FOOD_CODE, FOODS_CATEGORIES.CATEGORY_CODE)
                    .select(
                        context.select(inline(it.newCode), FOODS_CATEGORIES.CATEGORY_CODE)
                            .from(FOODS_CATEGORIES)
                            .where(FOODS_CATEGORIES.FOOD_CODE.eq(it.sourceCode))
                    )
            }

            // FIXME: Duplicate codes should produce more specific exceptions (currently triggers generic HTTP 500 in API server)
            context.batch(foodsQueries + foodAttributeQueries + foodCategoriesQueries).execute()

        } else
            logger.debug("Empty list")
    }

    fun copyFoods(foods: List<CopyFoodV2>) {
        foodDatabase.runTransaction {
            copyFoods(foods, it)
        }
    }

    fun copyLocalFoods(sourceLocale: String, destLocale: String, foods: List<CopyLocalV2>, context: DSLContext) {
        if (foods.isNotEmpty()) {

            val sourceFoodCodes = foods.map { it.sourceCode }

            val sourcePortionSizeMethods = getPortionSizeMethods(sourceFoodCodes, sourceLocale, context)
            val sourceAssociatedFoods = getAssociatedFoods(sourceFoodCodes, sourceLocale, context)
            val sourceBrands = getBrands(sourceFoodCodes, sourceLocale, context)
            val sourceNutrientMapping = getNutrientTableCodes(sourceFoodCodes, sourceLocale, context)

            val newLocalFoods = foods.map {

                val nutrientTableCodes = if (it.nutrientTableCodes.isEmpty())
                    sourceNutrientMapping[it.sourceCode] ?: error("Expected element not in map")
                else
                    it.nutrientTableCodes

                NewLocalFoodV2(
                    it.destCode, it.localDescription,
                    nutrientTableCodes,
                    sourcePortionSizeMethods[it.sourceCode] ?: error("Expected element not in map"),
                    sourceAssociatedFoods[it.sourceCode] ?: error("Expected element not in map"),
                    sourceBrands[it.sourceCode] ?: error("Expected element not in map")
                )
            }

            createLocalFoods(newLocalFoods, destLocale, context)
        }
    }

    fun copyCategoryLocal(sourceLocaleId: String, destLocaleId: String, context: DSLContext) {
        validateLocaleId(sourceLocaleId, context)
        validateLocaleId(destLocaleId, context)

        context.insertInto(
            CATEGORIES_LOCAL,
            CATEGORIES_LOCAL.CATEGORY_CODE,
            CATEGORIES_LOCAL.LOCALE_ID,
            CATEGORIES_LOCAL.LOCAL_DESCRIPTION,
            CATEGORIES_LOCAL.SIMPLE_LOCAL_DESCRIPTION,
            CATEGORIES_LOCAL.VERSION
        )
            .select(
                context.select(
                    CATEGORIES_LOCAL.CATEGORY_CODE,
                    inline(destLocaleId),
                    CATEGORIES_LOCAL.LOCAL_DESCRIPTION,
                    CATEGORIES_LOCAL.SIMPLE_LOCAL_DESCRIPTION,
                    Routines.uuidGenerateV4()
                )
                    .from(CATEGORIES_LOCAL)
                    .where(
                        CATEGORIES_LOCAL.LOCALE_ID.eq(sourceLocaleId)
                    )
            ).execute()
    }

    fun copyLocalFoods(sourceLocale: String, destLocale: String, foods: List<CopyLocalV2>) {
        foodDatabase.runTransaction {
            copyLocalFoods(sourceLocale, destLocale, foods, it)
        }
    }

    fun updateLocalFoods(updates: List<UpdateLocalFoodV2>, localeId: String, context: DSLContext) {
        updateNutrientTableCodes(updates.map { Pair(it.code, it.nutrientTableCodes) }, localeId, context)
        updateAssociatedFoods(updates.map { Pair(it.code, it.associatedFoods) }, localeId, context)
        updateBrands(updates.map { Pair(it.code, it.brandNames) }, localeId, context)
        updatePortionSizeMethods(updates.map { Pair(it.code, it.portionSize) }, localeId, context)

        val updateQueries = updates.map { update ->
            context.update(FOODS_LOCAL)
                .set(FOODS_LOCAL.FOOD_CODE, update.newCode)
                .set(FOODS_LOCAL.LOCAL_DESCRIPTION, truncateDescription(update.localDescription, update.code))
                .set(FOODS_LOCAL.SIMPLE_LOCAL_DESCRIPTION, truncateDescription(StringUtils.stripAccents(update.localDescription), update.code))
                .set(FOODS_LOCAL.VERSION, UUID.randomUUID())
                .where(FOODS_LOCAL.LOCALE_ID.eq(localeId).and(FOODS_LOCAL.FOOD_CODE.eq(update.code).and(FOODS_LOCAL.VERSION.eq(update.baseVersion))))
        }

        val updateResult = context.batch(updateQueries).execute().toTypedArray()

        val failed = updates.map { it.code }.zip(updateResult).filter { it.second == 0 }

        if (failed.isNotEmpty()) {
            throw IllegalArgumentException(
                "Base versions did not match for the following codes: ${
                    failed.map { it.first }.joinToString()
                }. Most likely cause is a concurrent update. Please retry."
            )
        }
    }

    private fun <T> nullableAsList(value: T?): List<T> {
        if (value == null)
            return emptyList()
        else
            return listOf(value)
    }

    fun getRootCategories(localeId: String): List<CategoryHeader> {
        return foodDatabase.runTransaction {

            validateLocaleId(localeId, it)

            val c = CATEGORIES.`as`("c")

            // A root category is a category that is
            //
            // (1) not a subcategory of any other category
            // (2) is a subcategory only of hidden categories
            //
            // Since (1) implies (2) it is only necessary to check (2)

            it.selectDistinct(c.CODE, c.DESCRIPTION, c.IS_HIDDEN, CATEGORIES_LOCAL.LOCAL_DESCRIPTION)
                .from(
                    c.leftJoin(CATEGORIES_CATEGORIES).on(c.CODE.eq(CATEGORIES_CATEGORIES.SUBCATEGORY_CODE))
                        .leftJoin(CATEGORIES_LOCAL).on(c.CODE.eq(CATEGORIES_LOCAL.CATEGORY_CODE))
                )
                .where(
                    notExists(
                        it.selectOne().from(CATEGORIES_CATEGORIES.innerJoin(CATEGORIES).on(CATEGORIES_CATEGORIES.CATEGORY_CODE.eq(CATEGORIES.CODE)))
                            .where(not(CATEGORIES.IS_HIDDEN)).and(CATEGORIES_CATEGORIES.SUBCATEGORY_CODE.eq(c.CODE))
                    )
                )
                .and(CATEGORIES_LOCAL.LOCALE_ID.eq(localeId))
                .orderBy(CATEGORIES_LOCAL.LOCAL_DESCRIPTION)
                .fetch {
                    CategoryHeader(it[c.CODE], it[c.DESCRIPTION], nullableAsList(it[CATEGORIES_LOCAL.LOCAL_DESCRIPTION]), it[c.IS_HIDDEN])
                }
        }
    }

    fun getCategoryCodes(context: DSLContext): Array<String> {
        return context.select(CATEGORIES.CODE).from(CATEGORIES).fetchArray(CATEGORIES.CODE)
    }


    fun getUncategorisedFoods(localeId: String): List<FoodHeader> {
        return foodDatabase.runTransaction {

            validateLocaleId(localeId, it)

            it.select(FOODS.CODE, FOODS.DESCRIPTION, FOODS_LOCAL.LOCAL_DESCRIPTION)
                .from(
                    FOODS.innerJoin(FOODS_LOCAL_LISTS).on(FOODS.CODE.eq(FOODS_LOCAL_LISTS.FOOD_CODE))
                        .leftJoin(FOODS_LOCAL).on(FOODS.CODE.eq(FOODS_LOCAL.FOOD_CODE))
                        .leftJoin(FOODS_CATEGORIES).on(FOODS.CODE.eq(FOODS_CATEGORIES.FOOD_CODE))
                )
                .where(FOODS_CATEGORIES.CATEGORY_CODE.isNull)
                .and(FOODS_LOCAL_LISTS.LOCALE_ID.eq(localeId))
                .and(FOODS_LOCAL.LOCALE_ID.eq(localeId))
                .orderBy(FOODS_LOCAL.LOCAL_DESCRIPTION)
                .fetch {
                    FoodHeader(it[FOODS.CODE], it[FOODS.DESCRIPTION], nullableAsList(it[FOODS_LOCAL.LOCAL_DESCRIPTION]))
                }
        }
    }

    fun getCategoryContents(categoryCode: String, localeId: String): CategoryContents {
        return foodDatabase.runTransaction { context ->

            val fl = FOODS_LOCAL.`as`("fl")
            val flp = FOODS_LOCAL.`as`("flp")

            val foodHeaders = context.select(FOODS.CODE, FOODS.DESCRIPTION, coalesce(fl.LOCAL_DESCRIPTION, flp.LOCAL_DESCRIPTION).`as`("local_desc"))
                .from(
                    FOODS_CATEGORIES.innerJoin(FOODS_LOCAL_LISTS).on(FOODS_CATEGORIES.FOOD_CODE.eq(FOODS_LOCAL_LISTS.FOOD_CODE))
                        .and(FOODS_LOCAL_LISTS.LOCALE_ID.eq(localeId))
                        .leftJoin(FOODS).on(FOODS.CODE.eq(FOODS_CATEGORIES.FOOD_CODE))
                        .leftJoin(fl).on(FOODS.CODE.eq(fl.FOOD_CODE)).and(fl.LOCALE_ID.eq(localeId))
                        .leftJoin(flp).on(FOODS.CODE.eq(flp.FOOD_CODE))
                        .and(flp.LOCALE_ID.`in`(context.select(LOCALES.PROTOTYPE_LOCALE_ID).from(LOCALES).where(LOCALES.ID.eq(localeId))))
                )
                .where(FOODS_CATEGORIES.CATEGORY_CODE.eq(categoryCode))
                .orderBy(coalesce(fl.LOCAL_DESCRIPTION, flp.LOCAL_DESCRIPTION))
                .fetch {
                    FoodHeader(it[FOODS.CODE], it[FOODS.DESCRIPTION], nullableAsList(it.get("local_desc", String::class.java)))
                }

            val categoryHeaders = context.select(CATEGORIES.CODE, CATEGORIES.DESCRIPTION, CATEGORIES_LOCAL.LOCAL_DESCRIPTION, CATEGORIES.IS_HIDDEN)
                .from(
                    CATEGORIES_CATEGORIES.leftJoin(CATEGORIES).on(CATEGORIES_CATEGORIES.SUBCATEGORY_CODE.eq(CATEGORIES.CODE))
                        .leftJoin(CATEGORIES_LOCAL)
                        .on(CATEGORIES_CATEGORIES.SUBCATEGORY_CODE.eq(CATEGORIES_LOCAL.CATEGORY_CODE).and(CATEGORIES_LOCAL.LOCALE_ID.eq(localeId)))
                )
                .where(CATEGORIES_CATEGORIES.CATEGORY_CODE.eq(categoryCode))
                .orderBy(CATEGORIES_LOCAL.LOCAL_DESCRIPTION)
                .fetch {
                    CategoryHeader(
                        it[CATEGORIES.CODE],
                        it[CATEGORIES.DESCRIPTION],
                        nullableAsList(it[CATEGORIES_LOCAL.LOCAL_DESCRIPTION]),
                        it[CATEGORIES.IS_HIDDEN]
                    )
                }

            CategoryContents(foodHeaders, categoryHeaders)
        }
    }

    fun getLocalFoodCodes(localeId: String): List<String> {
        return foodDatabase.runTransaction { context ->
            context.select(FOODS_LOCAL.FOOD_CODE)
                .from(FOODS_LOCAL)
                .where(FOODS_LOCAL.LOCALE_ID.eq(localeId))
                .orderBy(FOODS_LOCAL.FOOD_CODE.asc())
                .fetch(FOODS_LOCAL.FOOD_CODE)
        }
    }

    fun getEnabledLocalFoodCodes(localeId: String): List<String> {
        return foodDatabase.runTransaction { context ->
            context.select(FOODS_LOCAL_LISTS.FOOD_CODE)
                .from(FOODS_LOCAL_LISTS)
                .where(FOODS_LOCAL_LISTS.LOCALE_ID.eq(localeId))
                .orderBy(FOODS_LOCAL_LISTS.FOOD_CODE.asc())
                .fetch(FOODS_LOCAL_LISTS.FOOD_CODE)
        }
    }

    fun copyCategoryPortionSizeMethods(sourceLocale: String, destLocale: String) {
        foodDatabase.runTransaction {
            validateLocaleId(sourceLocale, it)
            validateLocaleId(destLocale, it)
            copyCategoryPortionSizeMethods(sourceLocale, destLocale, it)
        }
    }

    fun copyCategoryPortionSizeMethods(sourceLocale: String, destLocale: String, context: DSLContext) {
        val categoryCodes = getCategoryCodes(context).toList()
        val portionSizeMethods = getCategoryPortionSizeMethods(categoryCodes, sourceLocale, context)
            .toList()
            .filter { it.second.isNotEmpty() }
        updateCategoryPortionSizeMethods(portionSizeMethods, destLocale, context)
    }

    /*
    Temporary solution for locale initialization that copies local category names from sourceLocale to destLocale.
    Category structure will have to be separated per locale at some point.
     */
    fun copyCategories(sourceLocale: String, destLocale: String, context: DSLContext) {
        validateLocaleId(sourceLocale, context)
        validateLocaleId(destLocale, context)

        val sourceRows = context.select(CATEGORIES_LOCAL.CATEGORY_CODE, CATEGORIES_LOCAL.LOCAL_DESCRIPTION, CATEGORIES_LOCAL.SIMPLE_LOCAL_DESCRIPTION)
            .from(CATEGORIES_LOCAL)
            .where(CATEGORIES_LOCAL.LOCALE_ID.eq(sourceLocale))
            .fetchArray()

        val insert = context.insertInto(
            CATEGORIES_LOCAL, CATEGORIES_LOCAL.LOCALE_ID, CATEGORIES_LOCAL.CATEGORY_CODE,
            CATEGORIES_LOCAL.LOCAL_DESCRIPTION, CATEGORIES_LOCAL.SIMPLE_LOCAL_DESCRIPTION, CATEGORIES_LOCAL.VERSION
        )

        sourceRows.fold(insert) { query, row ->
            query.values(
                destLocale, row[CATEGORIES_LOCAL.CATEGORY_CODE], row[CATEGORIES_LOCAL.LOCAL_DESCRIPTION],
                row[CATEGORIES_LOCAL.SIMPLE_LOCAL_DESCRIPTION], UUID.randomUUID()
            )
        }.execute()
    }

    /* Helper method for package export tool */
    fun getAllCategoryCodes(): List<String> {
        return foodDatabase.runTransaction { context ->
            context.select(CATEGORIES.CODE).from(CATEGORIES).orderBy(CATEGORIES.CODE).fetch(CATEGORIES.CODE)
        }
    }
}
