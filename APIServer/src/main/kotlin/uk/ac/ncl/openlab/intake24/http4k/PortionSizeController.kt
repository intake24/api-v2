package uk.ac.ncl.openlab.intake24.http4k

import com.google.inject.Inject
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.path
import uk.ac.ncl.intake24.serialization.StringCodec
import uk.ac.ncl.openlab.intake24.services.PortionSizeMethodsService

class PortionSizeController @Inject constructor(
    private val portionSizeMethodsService: PortionSizeMethodsService,
    private val stringCodec: StringCodec
) {

    fun exportImageMap(user: Intake24User, request: Request): Response {
        val imageMapId = request.path("imageMapId")

        if (imageMapId == null)
            return Response(Status.BAD_REQUEST)
        else {
            val imageMap = portionSizeMethodsService.exportImageMap(imageMapId)

            if (imageMap == null)
                return Response(Status.NOT_FOUND)
            else
                return Response(Status.OK).body(stringCodec.encode(imageMap))
        }
    }

    fun exportDrinkwareSet(user: Intake24User, request: Request): Response {
        val drinkwareSetId = request.path("drinkwareId")

        if (drinkwareSetId == null)
            return Response(Status.BAD_REQUEST)
        else {
            val drinkwareSet = portionSizeMethodsService.exportDrinkwareSet(drinkwareSetId)

            if (drinkwareSet == null)
                return Response(Status.NOT_FOUND)
            else
                return Response(Status.OK).body(stringCodec.encode(drinkwareSet))
        }
    }
}
