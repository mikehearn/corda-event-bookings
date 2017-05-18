package com.r3.hackx.eventbookings

import co.paralleluniverse.fibers.Suspendable
import com.google.common.util.concurrent.Futures
import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.X509Utilities.getX509Name
import net.corda.core.crypto.commonName
import net.corda.core.crypto.orgName
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.getOrThrow
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.minutes
import net.corda.core.node.CordaPluginRegistry
import net.corda.core.node.PluginServiceHub
import net.corda.core.node.services.ServiceInfo
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
import net.corda.node.driver.driver
import net.corda.node.services.schema.HibernateObserver
import net.corda.node.services.transactions.ValidatingNotaryService
import net.corda.nodeapi.User
import org.hibernate.annotations.Immutable
import java.security.PublicKey
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.function.Function
import javax.persistence.*

data class EventBooking(
        val customers: List<AbstractParty>,
        val bookingAgency: AbstractParty? = null,
        val eventProvider: AbstractParty,
        val seatLocation: String? = null,
        val startTime: ZonedDateTime,
        val description: String,
        override val linearId: UniqueIdentifier
) : LinearState, QueryableState {
    override val contract = EventBookingContract()
    override val participants = customers + listOf(eventProvider, bookingAgency).filterNotNull()

    override fun isRelevant(ourKeys: Set<PublicKey>) = ourKeys.intersect(participants.map { it.owningKey }).isNotEmpty()

    override fun supportedSchemas() = listOf(EventBookingSchemaV1)
    override fun generateMappedObject(schema: MappedSchema) = EventBookingSchemaV1.EventBookingPersistentState(this)

    object EventBookingSchemaV1 : MappedSchema(EventBooking::class.java, 1, listOf(EventBookingPersistentState::class.java)) {
        @Entity @Table(name = "event_bookings")
        class EventBookingPersistentState(booking: EventBooking) : PersistentState() {
            @Column var customers: String = booking.customers.map { it.nameOrNull()!!.commonName }.joinToString()
            @Column var eventProvider: String = booking.eventProvider.nameOrNull()!!.commonName
            @Column var startTime: ZonedDateTime = booking.startTime
        }
    }
}

class EventBookingContract : Contract {
    override val legalContractReference: SecureHash = SecureHash.sha256("dummy")

    interface BookingCommand : CommandData
    class NewBooking : BookingCommand, TypeOnlyCommandData()
    class CancelBooking : BookingCommand, TypeOnlyCommandData()
    class AdjustBooking : BookingCommand, TypeOnlyCommandData()

    override fun verify(tx: TransactionForContract) {
        val command = tx.commands.requireSingleCommand<BookingCommand>()
        when (command.value) {
            is NewBooking -> verifyNewBooking(tx, command.signers)
            is CancelBooking -> verifyCancellation(tx, command.signers)
            is AdjustBooking -> verifyAdjustment(tx, command.signers)
            else -> throw IllegalStateException("Unknown command $command")
        }
    }

    private fun verifyAdjustment(tx: TransactionForContract, signers: List<PublicKey>) {
        val input = tx.inputs.filterIsInstance<EventBooking>().single()
        val output = tx.outputs.filterIsInstance<EventBooking>().single()
        requireThat {
            "Adjustment cannot change the booking reference" using (input.linearId == output.linearId)
            "Adjustments can only be made by the event provider" using (input.eventProvider.owningKey in signers)
            "Adjustments can only change the seat number" using (input.copy(seatLocation = null) == output.copy(seatLocation = null))
            if (input.seatLocation != null)
                "The provider cannot take away your seat once allocated" using (output.seatLocation != null)
        }
    }

    private fun verifyNewBooking(tx: TransactionForContract, signers: List<PublicKey>) {
        check(tx.inputs.none { it is EventBooking }) { "New booking transactions may not refer to a prior booking"}

        val newBookings = tx.outputs.filterIsInstance<EventBooking>()
        check(newBookings.isNotEmpty()) { "There must be at least one new booking" }

        for ((customers, bookingAgency, eventProvider, _, startTime) in newBookings) {
            val txTime = tx.timestamp?.before ?: throw IllegalStateException("Bookings must be timestamped")
            requireThat {
                "Bookings have at least one customer" using customers.isNotEmpty()
                "Bookings cannot be made for the past" using (txTime < startTime.toInstant())
                if (bookingAgency != null)
                    "New bookings are signed by the agency" using (bookingAgency.owningKey in signers)
                else
                    "New bookings are signed by the customers" using (signers.containsAll(customers.map { it.owningKey }))
                "New bookings are signed by the event provider" using (eventProvider.owningKey in signers)
            }
        }
    }

    private fun verifyCancellation(tx: TransactionForContract, signers: List<PublicKey>) {
        val cancellations = tx.inputs.filterIsInstance<EventBooking>()
        check(cancellations.isNotEmpty()) { "Must be at least one cancellation" }

        check(tx.outputs.none { it is EventBooking }) { "Cancellation transactions may not have any booking outputs"}

        for (c in cancellations) {
            val txTime = tx.timestamp?.before ?: throw IllegalStateException("Cancellations must be timestamped")
            requireThat {
                "You cannot cancel an event that has already started" using (txTime < c.startTime.toInstant())
            }
        }
    }
}

class NewBookingFlow {
    @CordaSerializable
    data class Suggestion(val eventStartTime: ZonedDateTime, val suggestedSeat: String? = null)

    @InitiatingFlow @StartableByRPC
    open class Initiator(val forEventDescription: String, val organiser: Party) : FlowLogic<EventBooking>() {
        companion object {
            object RequestingSuggestion : ProgressTracker.Step("Requesting time and seat suggestion")

            object CollectingSignatures : ProgressTracker.Step("Collecting signatures") {
                override fun childProgressTracker() = CollectSignaturesFlow.tracker()
            }

            object Finalising : ProgressTracker.Step("Finalising transaction") {
                override fun childProgressTracker() = FinalityFlow.tracker()
            }

            fun tracker() = ProgressTracker(RequestingSuggestion, CollectingSignatures, Finalising)
        }

        override val progressTracker = tracker()

        @Suspendable
        override fun call(): EventBooking {
            // Ask them to suggest a time and seat. Sub-classes could override this if they wanted to customise
            // its behaviour.
            progressTracker.currentStep = RequestingSuggestion
            val suggestion = sendAndReceive<Suggestion>(organiser, forEventDescription).unwrap {
                checkSuggestion(it)
                it
            }

            // I'm going to create a transaction with the suggested proposal and try to collect the signature of the
            // event organiser. They can reject it if the transaction isn't OK.
            val me = serviceHub.myInfo.legalIdentity
            val booking = EventBooking(
                    customers = listOf(me),
                    eventProvider = organiser,
                    description = forEventDescription,
                    seatLocation = suggestion.suggestedSeat,
                    startTime = suggestion.eventStartTime,
                    linearId = UniqueIdentifier()
            )
            val tx = TransactionBuilder(notary = serviceHub.networkMapCache.getAnyNotary()).also {
                it.setTime(Instant.now(), 2.minutes)
                it.addCommand(EventBookingContract.NewBooking(), organiser.owningKey, me.owningKey)
                it.addOutputState(booking)
                it.signWith(serviceHub.legalIdentityKey)
            }.toSignedTransaction(checkSufficientSignatures = false)

            // Now get the other side to sign it, and then we will finalise it (notarise and return to the participants).
            progressTracker.currentStep = CollectingSignatures
            val signedByOrganiser = subFlow(CollectSignaturesFlow(tx, CollectingSignatures.childProgressTracker()))
            progressTracker.currentStep = Finalising
            subFlow(FinalityFlow(listOf(signedByOrganiser), emptySet(), Finalising.childProgressTracker()))
            return booking
        }

        protected fun checkSuggestion(suggestion: Suggestion) {
            // Nothing by default.
        }
    }

    class Responder(val counterParty: Party) : FlowLogic<Unit>() {
        val events = setOf(
                "Amazing flamenco!",
                "Even more flamenco!",
                "You have no clue how much flamenco we have tonight!"
        )

        @Suspendable
        override fun call() {
            val desiredEvent = receive<String>(counterParty).unwrap { it }
            if (desiredEvent !in events)
                throw FlowException("Unknown event '$desiredEvent'")
            val seatNumber = "A1"  // TODO: Use the db to reserve.
            val tomorrowEvening = serviceHub.clock.instant()
                    .atZone(ZoneId.of("CET"))
                    .plusDays(1)    // TODO: Use Strata holiday calendars.
                    .withHour(19)
                    .withMinute(30)

            send(counterParty, Suggestion(tomorrowEvening, seatNumber))

            var booking: EventBooking? = null
            subFlow(object : SignTransactionFlow(counterParty) {
                override fun checkTransaction(stx: SignedTransaction) {
                    // Check the proposed booking. The contract logic was already satisfied. Note that this could be
                    // ANY transaction, even a malicious one that tries to steal our money! So we gotta check the fine
                    // print to ensure it's what we expect.
                    booking = stx.tx.outputs.singleOrNull()?.data as? EventBooking
                    requireThat {
                        "Exactly one booking state is created" using (booking != null)
                        "This is a new booking transaction" using (stx.tx.commands.single().value is EventBookingContract.NewBooking)
                        "No inputs" using stx.tx.inputs.isEmpty()
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
    override val servicePlugins = listOf(Function(::EventBookingsService))
}
