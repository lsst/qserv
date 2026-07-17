/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CCONFIG_EVENTSERVICE_H
#define LSST_QSERV_CCONFIG_EVENTSERVICE_H

// System headers
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "cconfig/DataManagementEvent.h"

// This header declarations
namespace lsst::qserv::cconfig {

/**
 * Exception class for errors related to the EventService.
 */
class EventServiceException : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Class EventService is an abstraction for the service handling events posted by
 * the Replication system. It allows clients to interact with the event service, such as
 * posting new events and subscribing to event notifications.
 *
 * - Subscribers are expected to provide a callback function that will be invoked whenever
 *   a new event is posted and a unique identifier (subscription ID) for managing their
 *   subscription.
 * - Notifications on the new events will be sent to all subscribed clients asynchronously
 *   in the background using a pool of BOOST ASIO service threads. The size of the thread pool
 *   is specified during the creation of the EventService instance.
 * - The service needs to be explicitly started (or restarted if it was stopped earlier) by calling
 *   the start() method before it can handle events.
 *   Events will be ignored if the service has not been started.
 * - The service can be stopped by calling the stop() method, which will terminate all background
 *   threads and prevent further event processing.
 */
class EventService : public std::enable_shared_from_this<EventService> {
public:
    using OnEventCallback = std::function<void(DataManagementEvent)>;

    /**
     * Create a new instance of the EventService with the specified number of threads.
     * @param numThreads The number of threads to be used for handling events asynchronously.
     *   Must be greater than zero.
     * @return A shared pointer to the newly created EventService instance.
     * @throws std::invalid_argument if numThreads is zero.
     */
    static std::shared_ptr<EventService> create(std::size_t numThreads);

    EventService() = delete;
    EventService(EventService const&) = delete;
    EventService& operator=(EventService const&) = delete;
    ~EventService() noexcept;

    /**
     * Check if the EventService is currently running.
     * @return True if the service is running, false otherwise.
     */
    bool isRunning() const;

    /**
     * Start the EventService, allowing it to handle events and notify subscribers.
     * @throws EventServiceException if the service is already running.
     */
    void start();

    /**
     * Stop the EventService, terminating all background threads and preventing further event processing.
     * @throws EventServiceException if the service is not running.
     */
    void stop();

    /**
     * Post a new event to the EventService. The event will be processed asynchronously,
     * and all subscribed clients will be notified via their callback functions.
     * @param event The DataManagementEvent to be posted.
     * @throws std::invalid_argument if the event type is NONE.
     */
    void postEvent(DataManagementEvent const& event);

    /**
     * Subscribe to event notifications with a callback function and a unique subscription ID.
     * The callback will be invoked whenever a new event is posted.
     * @param callback The callback function to be called on new events.
     * @param subscriptionId A unique identifier for the subscription. Must not be empty.
     * @throws std::invalid_argument if the subscription ID is empty.
     * @throws EventServiceException if the subscription ID already exists.
     */
    void subscribe(OnEventCallback const& callback, std::string const& subscriptionId);

    /**
     * Unsubscribe from event notifications using the subscription ID.
     * @param subscriptionId The unique identifier of the subscription to be removed. Must not be empty.
     * @throws std::invalid_argument if the subscription ID is empty.
     * @throws EventServiceException if the subscription ID does not exist.
     */
    void unsubscribe(std::string const& subscriptionId);

private:
    EventService(std::size_t numThreads);

    std::size_t const _numThreads;                         ///< The number of the BOOST ASIO service threads.
    mutable std::mutex _mtx;                               ///< The mutex protecting the object state.
    std::unique_ptr<boost::asio::io_service> _io_service;  ///< The BOOST ASIO I/O services.
    std::unique_ptr<boost::asio::io_service::work> _work;  ///< To keep the I/O service running.

    /// The ASIO thread pool for sending asynchronous notifications to subscribers.
    std::vector<std::unique_ptr<std::thread>> _threads;

    /// The map of subscription IDs to their corresponding callback functions.
    std::map<std::string, OnEventCallback> _subscriptions;
};

}  // namespace lsst::qserv::cconfig

#endif  // LSST_QSERV_CCONFIG_EVENTSERVICE_H
