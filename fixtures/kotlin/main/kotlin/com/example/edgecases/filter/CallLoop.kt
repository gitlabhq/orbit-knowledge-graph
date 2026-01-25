package smoketest.coroutines

import kotlinx.coroutines.flow.flow

class CoroutinesController {
    fun a() = flow {
        
    }

	fun flow() = flow {
		emit("World")
	}
}
