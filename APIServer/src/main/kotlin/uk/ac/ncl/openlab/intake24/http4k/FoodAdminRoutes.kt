package uk.ac.ncl.openlab.intake24.http4k

import com.google.inject.Inject
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.routing.bind
import org.http4k.routing.path
import org.http4k.routing.routes

class FoodAdminRoutes @Inject() constructor(
    foodsController: FoodsController,
    deriveLocaleController: DeriveLocaleController,
    mergeLocalesController: MergeLocalesController,
    uksaUpdatePsmController: UKSAUpdatePsmController,
    foodFrequencyStatsController: FoodFrequencyStatsController,
    nutrientMappingController: NutrientMappingController,
    security: Security
) {


    private fun canWriteLocalFoods(user: Intake24User, request: Request): Boolean {
        val localeId = request.path("localeId")

        if (localeId == null)
            throw IllegalArgumentException("Request must contain localeId path parameter")
        else
            return user.hasRole(Intake24Roles.foodsAdmin) or user.hasRole(Intake24Roles.foodDatabaseMaintainer(localeId))
    }

    private fun canReadLocalFoods(user: Intake24User, request: Request): Boolean {
        val localeId = request.path("localeId")
        if (localeId == null)
            throw IllegalArgumentException("Request must contain localeId path parameter")
        else
            return user.hasRole(Intake24Roles.foodsAdmin) or user.hasRole(Intake24Roles.foodDatabaseMaintainer(localeId))
    }

    val router =
        routes(
            "/{localeId}/local-food-codes" bind Method.GET to security.check(::canReadLocalFoods, foodsController::getLocalFoodCodes),
            "/{localeId}/enabled-local-food-codes" bind Method.GET to security.check(::canReadLocalFoods, foodsController::getEnabledLocalFoodCodes),
            "/{localeId}/root-categories" bind Method.GET to security.check(::canReadLocalFoods, foodsController::getRootCategories),
            "/{localeId}/uncategorised-foods" bind Method.GET to security.check(::canReadLocalFoods, foodsController::getUncategorisedFoods),
            "/{localeId}/categories/{category}/contents" bind Method.GET to security.check(::canReadLocalFoods, foodsController::getCategoryContents),

            "/copy" bind Method.POST to security.allowFoodAdmins(foodsController::copyFoods),
            "/copy-local" bind Method.POST to security.allowFoodAdmins(foodsController::copyLocalFoods),
            "/clone-local" bind Method.POST to security.allowFoodAdmins(deriveLocaleController::cloneLocalFoods),
            "/derive-locale" bind Method.POST to security.allowFoodAdmins(deriveLocaleController::deriveLocale),
            "/merge-locales" bind Method.POST to security.allowFoodAdmins(mergeLocalesController::mergeLocales),
            "/update-uk-sa-psm" bind Method.POST to security.allowFoodAdmins(uksaUpdatePsmController::updatePortionSizes),
            "/frequencies" bind Method.POST to security.allowFoodAdmins(foodFrequencyStatsController::exportFrequencies),
            "/{localeId}/export-mapping" bind Method.POST to security.check(::canReadLocalFoods, nutrientMappingController::exportMapping),
            "/copy-category-psm" bind Method.POST to security.allowFoodAdmins(foodsController::copyCategoryPortionSizeMethods)
        )
}
