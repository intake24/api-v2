package uk.ac.ncl.openlab.intake24.http4k

import com.google.inject.Inject
import org.http4k.core.MultipartFormBody
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.slf4j.LoggerFactory
import uk.ac.ncl.intake24.serialization.StringCodec
import uk.ac.ncl.openlab.intake24.tools.*

class DeriveLocaleController @Inject constructor(
    private val service: DeriveLocaleService,
    private val fctService: FoodCompositionTableService,
    private val errorUtils: ErrorUtils,
    private val stringCodec: StringCodec
) {

    private val logger = LoggerFactory.getLogger(DeriveLocaleController::class.java)

    private data class ErrorsResponse(val errors: List<String>)

    fun deriveLocale(user: Intake24User, request: Request): Response {

        val form = MultipartFormBody.from(request)
        val file = form.file("file")
        val sourceLocale = form.field("sourceLocale")?.value
        val targetLocale = form.field("targetLocale")?.value
        val format = form.field("format")?.value

        if (file == null || sourceLocale == null || targetLocale == null || format == null)
            return errorUtils.errorResponse(Status.BAD_REQUEST, "Missing required field")
        else {

            try {

                val (errors, actions) = when (format) {
                    "ndns1" -> DeriveLocaleNDNSCsvParser.parseTable(file.content)
                    "sab1" -> DeriveLocaleSABCsvParser.parseTable(file.content)
                    "nz1" -> DeriveLocaleNZCsvParser.parseTable(file.content)
                    else -> throw DeriveLocaleParseException("Unexpected format value: $format")
                }

                val codes = actions.mapNotNull {
                    when (it) {
                        is FoodAction.Include -> it.localFctCode
                        is FoodAction.New -> it.fctCode
                        is FoodAction.Clone -> it.fctReference
                        is FoodAction.NoAction -> null
                    }
                }

                val codeErrors = fctService.checkFoodCompositionCodes(codes).map {
                    "Food composition record ${it.recordId} does not exist in table ${it.tableId}"
                }.distinct().sorted()

                val allErrors = errors + codeErrors

                if (allErrors.isNotEmpty())
                    return Response(Status.BAD_REQUEST).body(stringCodec.encode(ErrorsResponse(allErrors)))
                else {
                    try {
                        service.deriveLocale(sourceLocale, targetLocale, actions)
                        return Response(Status.OK)
                    } catch (e: DeriveLocaleException) {
                        return Response(Status.BAD_REQUEST).body(stringCodec.encode(ErrorsResponse(e.errors)))
                    }
                }
            } catch (e: DeriveLocaleParseException) {
                return errorUtils.errorResponse(Status.BAD_REQUEST, e)
            }
        }
    }

    fun cloneLocalFoods(user: Intake24User, request: Request): Response {
        val form = MultipartFormBody.from(request)
        val sourceLocale = form.field("sourceLocale")?.value
        val targetLocale = form.field("targetLocale")?.value

        if (sourceLocale == null || targetLocale == null)
            return errorUtils.errorResponse(Status.BAD_REQUEST, "Missing required field")
        else {
            service.cloneLocalFoods(sourceLocale, targetLocale)
            return Response(Status.OK)
        }

    }
}
