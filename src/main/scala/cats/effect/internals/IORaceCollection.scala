package cats.effect.internals

import java.util.concurrent.atomic.AtomicBoolean
import cats.effect.{ContextShift, IO, IOInstances, Timer}
import scala.concurrent.ExecutionContext

object IORaceCollection {

	def raceMany[T](tasks: Seq[IO[T]])(implicit cs: ContextShift[IO]): IO[Either[Throwable, T]] = IO.Async(
		trampolineAfter = true,
		k = (_, cb) => {
			val active = new AtomicBoolean(true)
			val stackSize = tasks.length

			val connections = tasks.map { _ ⇒
				IOConnection()
			}

			(0 until stackSize).foreach { i ⇒
				IORunLoop.startCancelable[T](IOForkedStart(tasks(i), cs), connections(i), {
					case Left(error)                => onError(active, connections.slice(0, i), connections.slice(i + 1, stackSize), cb, error)
					case right: Right[Throwable, T] => onSuccess(active, connections.slice(0, i), connections.slice(i + 1, stackSize), cb, right)
				})
			}
		}
	)

	private[this] def onSuccess[T](active: AtomicBoolean, othersHead: Seq[IOConnection], othersTail: Seq[IOConnection], cb: Callback.T[Either[Throwable, T]], r: Right[Throwable, T]): Unit = if (active.getAndSet(false)) {
		cancelIOConnections(othersHead)
		cancelIOConnections(othersTail)
		cb(Right(r))
	}

	private[this] def onError[T](active: AtomicBoolean, othersHead: Seq[IOConnection], othersTail: Seq[IOConnection], cb: Callback.T[T], err: Throwable): Unit = if (active.getAndSet(false)) {
		cancelIOConnections(othersHead)
		cancelIOConnections(othersTail)
		cb(Left(err))
	} else {
		Logger.reportFailure(err)
	}

	private[this] def cancelIOConnections(connections: Seq[IOConnection]): Unit = connections.foreach { o ⇒
		o.cancel.unsafeRunAsync {
			case Left(e) => Logger.reportFailure(e)
			case _       => ()
		}
	}
}

object IOImplicits {

	implicit final class RichIOInstances(companion: IOInstances) {

		def raceMany[T](tasks: Seq[IO[T]])(implicit contextShift: ContextShift[IO]): IO[Either[Throwable, T]] = IORaceCollection.raceMany(tasks)

	}
}

object Program {
	import cats.syntax.all._
	import IOImplicits.RichIOInstances
	import scala.concurrent.duration.DurationInt

	implicit private val timer: Timer[IO] = IO.timer(ExecutionContext.global)
	implicit private val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

	def main(args: Array[String]): Unit = {
		val ioA = timer.sleep(5.second) *> io(0)
		val ioB = timer.sleep(9.second) *> io(1)
		val ioC = timer.sleep(1.second) *> io(2, Some(new NullPointerException()))
		val ioD = timer.sleep(3.second) *> io(3)
		val ioE = timer.sleep(1.second) *> io(4)

		IORaceCollection.raceMany(List(ioA, ioB, ioC, ioD, ioE)).handleError(t ⇒ Left[Throwable, Int](t)).unsafeRunSync().fold(
			println,
			println
		)

		IO.raceMany(List(ioA, ioB, ioC, ioD, ioE)).handleError(t ⇒ Left[Throwable, Int](t)).unsafeRunSync().fold(
			println,
			println
		)

		IO.never.unsafeRunSync()
	}

	private def io(i: Int, error: Option[RuntimeException] = None) = IO[Int] {
		println(s"Doing #0$i...")
		error.map(throw _).getOrElse(i)
	}
}
