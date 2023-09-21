package uk.ac.ncl.openlab.intake24.http4k

import com.google.inject.Inject
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.path
import uk.ac.ncl.intake24.serialization.StringCodec
import uk.ac.ncl.openlab.intake24.services.CopyFoodV2
import uk.ac.ncl.openlab.intake24.services.CopyLocalV2
import uk.ac.ncl.openlab.intake24.services.FoodsServiceV2


data class CopyFoodsV2Request(val foods: List<CopyFoodV2>)

data class CopyLocalFoodsV2Request(val sourceLocale: String, val destLocale: String, val foods: List<CopyLocalV2>)

data class LocalFoodCodesResponse(val localFoodCodes: List<String>)

class FoodsController @Inject constructor(
    private val service: FoodsServiceV2,
    private val stringCodec: StringCodec,
    private val errorUtils: ErrorUtils
) {


    private fun validateLocaleId(request: Request, onValid: (String) -> Response): Response {
        val localeId = request.path("localeId")

        return if (localeId == null)
            errorUtils.errorResponse(Status.BAD_REQUEST, "Locale id missing")
        else
            onValid(localeId)
    }

    fun copyFoods(user: Intake24User, request: Request): Response {
        val request = stringCodec.decode(request.bodyString(), CopyFoodsV2Request::class)
        service.copyFoods(request.foods)
        return Response(Status.OK)
    }

    fun copyLocalFoods(user: Intake24User, request: Request): Response {
        val request = stringCodec.decode(request.bodyString(), CopyLocalFoodsV2Request::class)
        service.copyLocalFoods(request.sourceLocale, request.destLocale, request.foods)
        return Response(Status.OK)
    }

    fun copyCategoryPortionSizeMethods(user: Intake24User, request: Request): Response {
        val srcLocaleId = request.query("src")
        val destLocaleId = request.query("dst")

        if (srcLocaleId == null || destLocaleId == null)
            return Response(Status.BAD_REQUEST)

        service.copyCategoryPortionSizeMethods(srcLocaleId, destLocaleId)
        return Response(Status.OK)
    }

    fun getRootCategories(user: Intake24User, request: Request): Response {
        return validateLocaleId(request) {
            Response(Status.OK).body(stringCodec.encode(service.getRootCategories(it)))
        }
    }

    fun getUncategorisedFoods(user: Intake24User, request: Request): Response {
        return validateLocaleId(request) {
            Response(Status.OK).body(stringCodec.encode(service.getUncategorisedFoods(it)))
        }
    }

    fun getCategoryContents(user: Intake24User, request: Request): Response {
        return validateLocaleId(request) { localeId ->

            val categoryCode = request.path("category")

            if (categoryCode == null)
                errorUtils.errorResponse(Status.BAD_REQUEST, "Category code is missing")
            else
                Response(Status.OK).body(stringCodec.encode(service.getCategoryContents(categoryCode, localeId)))
        }
    }

    fun getLocalFoodCodes(user: Intake24User, request: Request): Response {
        return validateLocaleId(request) { localeId ->
            Response(Status.OK).body(stringCodec.encode(LocalFoodCodesResponse(service.getLocalFoodCodes(localeId))))
        }
    }

    fun getEnabledLocalFoodCodes(user: Intake24User, request: Request): Response {
        return validateLocaleId(request) { localeId ->
            Response(Status.OK).body(stringCodec.encode(LocalFoodCodesResponse(service.getEnabledLocalFoodCodes(localeId))))
        }
    }
}
