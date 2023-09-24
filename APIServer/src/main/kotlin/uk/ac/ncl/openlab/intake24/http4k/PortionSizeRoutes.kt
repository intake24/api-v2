package uk.ac.ncl.openlab.intake24.http4k

import com.google.inject.Inject
import org.http4k.core.Method
import org.http4k.routing.bind
import org.http4k.routing.routes

class PortionSizeRoutes @Inject() constructor(
    portionSizeController: PortionSizeController,
    security: Security
) {

    val router = routes(
        "/image-maps/{imageMapId}/export" bind Method.GET to security.allowFoodAdmins(portionSizeController::exportImageMap),
        "/drinkware/{drinkwareId}/export" bind Method.GET to security.allowFoodAdmins(portionSizeController::exportDrinkwareSet),
    )
}
