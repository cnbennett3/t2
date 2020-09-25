import org.opencypher.morpheus.api.MorpheusSession
import org.renci.t2.core.Core

implicit val morpheus: MorpheusSession = MorpheusSession.local()

val core: Core = new Core(morpheus)

val g = core.makeGraph("v0.1" )