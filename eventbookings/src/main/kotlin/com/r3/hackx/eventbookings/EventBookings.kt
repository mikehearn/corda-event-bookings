package com.r3.hackx.eventbookings

import co.paralleluniverse.fibers.Suspendable
import com.google.common.net.HostAndPort
import javafx.collections.FXCollections
import javafx.scene.Parent
import javafx.scene.control.TableView
import javafx.scene.layout.StackPane
import net.corda.client.jfx.utils.observeOnFXThread
import net.corda.client.rpc.CordaRPCClient
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.commonName
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.minutes
import net.corda.core.node.CordaPluginRegistry
import net.corda.core.node.PluginServiceHub
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.QueryableState
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import net.corda.flows.CollectSignaturesFlow
import net.corda.flows.FinalityFlow
import net.corda.flows.SignTransactionFlow
import tornadofx.*
import java.security.PublicKey
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.util.function.Function
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Table

data class EventBooking(
        val customer: AbstractParty,
        val organiser: AbstractParty,
        val seatLocation: String? = null,
        val startTime: ZonedDateTime,
        val description: String
) : ContractState, QueryableState {
    override val contract get() = EventBookingContract()
    override val participants = listOf(customer, organiser)

    override fun supportedSchemas() = listOf(EventBookingSchemaV1)
    override fun generateMappedObject(schema: MappedSchema) = EventBookingSchemaV1.EventBookingEntity(this)

    object EventBookingSchemaV1 : MappedSchema(EventBooking::class.java, 1, listOf(EventBookingEntity::class.java)) {
        @Entity @Table(name = "event_bookings")
        class EventBookingEntity(booking: EventBooking) : PersistentState() {
            @Column var customer: String = booking.customer.nameOrNull()!!.commonName
            @Column var description: String = booking.description
            @Column var startTime: ZonedDateTime = booking.startTime
            @Column var seatLocation: String? = booking.seatLocation
        }
    }
}

class EventBookingContract : Contract {
    override val legalContractReference = SecureHash.zeroHash

    interface BookingCommand : CommandData
    class NewBooking : BookingCommand, TypeOnlyCommandData()
    class CancelBooking : BookingCommand, TypeOnlyCommandData()

    override fun verify(tx: TransactionForContract) {
        val command = tx.commands.requireSingleCommand<BookingCommand>()
        when (command.value) {
            is NewBooking -> verifyNewBooking(tx, command.signers)
            is CancelBooking -> verifyCancelBooking(tx, command.signers)
            else -> throw AssertionError()
        }
    }

    private fun verifyNewBooking(tx: TransactionForContract, signers: List<PublicKey>) {
        check(tx.inputs.isEmpty())
        val newBooking = tx.outputs.single() as EventBooking
        val txTime = tx.timestamp?.before ?: throw IllegalStateException("New bookings must be timestamped")
        requireThat {
            "Bookings may not be in the past" using (txTime < newBooking.startTime.toInstant())
            "New bookings must be signed by the organiser" using (newBooking.organiser.owningKey in signers)
        }
    }

    private fun verifyCancelBooking(tx: TransactionForContract, signers: List<PublicKey>) {
        check(tx.outputs.isEmpty())
        val booking = tx.inputs.single() as EventBooking
        val txTime = tx.timestamp?.before ?: throw IllegalStateException("Cancellations must be timestamped")
        requireThat {
            "Cancellation must take place at least two hours before" using (txTime < booking.startTime.minusHours(2).toInstant())
            val didCustomerSign = booking.customer.owningKey in signers
            val didOrganiserSign = booking.organiser.owningKey in signers
            "Must be signed by customer or organiser" using (didCustomerSign || didOrganiserSign)
        }
    }
}

class NewBookingFlow {
    @CordaSerializable
    data class Suggestion(val startTime: ZonedDateTime, val seatLocation: String?)

    @InitiatingFlow @StartableByRPC
    open class Initiator(val forEvent: String, val organiser: Party) : FlowLogic<Unit>() {
        companion object {
            object REQUESTING_SUGGESTION : ProgressTracker.Step("Requesting time and seat suggestion")

            object COLLECTING_SIGNATURES : ProgressTracker.Step("Collecting signatures") {
                override fun childProgressTracker() = CollectSignaturesFlow.tracker()
            }

            object FINALISING : ProgressTracker.Step("Finalising transaction") {
                override fun childProgressTracker() = FinalityFlow.tracker()
            }

            fun tracker() = ProgressTracker(REQUESTING_SUGGESTION, COLLECTING_SIGNATURES, FINALISING)
        }

        override val progressTracker = tracker()

        @Suspendable
        override fun call() {
            progressTracker.currentStep = REQUESTING_SUGGESTION
            val suggestion = sendAndReceive<Suggestion>(organiser, forEvent).unwrap {
                checkSuggestion(it)
                it
            }

            val booking = EventBooking(
                    customer = serviceHub.myInfo.legalIdentity,
                    organiser = organiser,
                    seatLocation = suggestion.seatLocation,
                    startTime = suggestion.startTime,
                    description = forEvent
            )
            val txb = TransactionBuilder(notary = serviceHub.networkMapCache.getAnyNotary()).also {
                it.setTime(serviceHub.clock.instant(), 2.minutes)
                it.addCommand(EventBookingContract.NewBooking(), organiser.owningKey)
                it.addOutputState(booking)
            }
            val stx = serviceHub.signInitialTransaction(txb)

            progressTracker.currentStep = COLLECTING_SIGNATURES
            val signedByOrganiser = subFlow(CollectSignaturesFlow(stx, COLLECTING_SIGNATURES.childProgressTracker()))
            progressTracker.currentStep = FINALISING
            subFlow(FinalityFlow(signedByOrganiser, FINALISING.childProgressTracker()))
        }

        protected fun checkSuggestion(suggestion: Suggestion) = Unit
    }

    class Responder(val counterParty: Party) : FlowLogic<Unit>() {
        val events = setOf(
                "Amazing flamenco",
                "Even more flamenco",
                "Flamenco until your ears bleed"
        )

        @Suspendable
        override fun call() {
            val desiredEvent = receive<String>(counterParty).unwrap { it }
            if (desiredEvent !in events)
                throw FlowException("Unknown event '$desiredEvent'")
            val seatLocation = "A1"
            val tomorrowEvening = serviceHub.clock.instant()
                    .atZone(ZoneId.of("CET"))
                    .plusDays(1)
                    .withHour(19)
                    .withMinute(30)

            send(counterParty, Suggestion(tomorrowEvening, seatLocation))

            subFlow(object : SignTransactionFlow(counterParty) {
                override fun checkTransaction(stx: SignedTransaction) {
                    val booking = stx.tx.outputs.single().data as EventBooking
                    requireThat {
                        "Must be a new booking transaction" using (stx.tx.commands.single().value is EventBookingContract.NewBooking)
                        "Must be a booking intended for us" using (booking.organiser == serviceHub.myInfo.legalIdentity)
                    }
                }
            })
        }
    }
}

// This stuff should all go away soon.
class EventBookingsService(services: PluginServiceHub) {
    init {
        services.registerServiceFlow(NewBookingFlow.Initiator::class.java) { NewBookingFlow.Responder(it) }
    }
}

class EventBookingsPlugin : CordaPluginRegistry() {
    override val servicePlugins: List<Function<PluginServiceHub, out Any>> = listOf(Function(::EventBookingsService))
}

class BookingsView : View("Bookings") {
    override val root by fxml<StackPane>()
    private val txTable by fxid<TableView<EventBooking>>()

    init {
        with(txTable) {
            columnResizePolicy = SmartResize.POLICY
            makeIndexColumn()
            column("Event", EventBooking::description)
            column("Start time", EventBooking::startTime) {
                cellFormat {
                    text = it.format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM))
                }
            }
            column("Customer", EventBooking::customer) {
                cellFormat {
                    text = it.nameOrNull()!!.commonName
                }
            }
        }

        val rpc = CordaRPCClient(HostAndPort.fromString("localhost:10009"))
        val proxy = rpc.start("guest", "letmein").proxy
        val (vault, updates) = proxy.vaultAndUpdates()

        val bookings = FXCollections.observableArrayList(vault.map { it.state.data }.filterIsInstance<EventBooking>())
        updates.observeOnFXThread().subscribe { update ->
            val cancelled = update.consumedOfType<EventBooking>()
            val booked = update.producedOfType<EventBooking>()
            bookings.removeAll(cancelled)
            bookings.addAll(booked)
        }

        txTable.items = bookings
    }
}

class BookingsApp : App(BookingsView::class) {
    init {
        importStylesheet("/com/r3/hackx/eventbookings/style.css")
    }
}