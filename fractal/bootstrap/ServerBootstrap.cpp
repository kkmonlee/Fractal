#pragma once

#include "../../folly/folly/wangle/bootstrap/ServerBootstrap.h"
#include "../../folly/folly/wangle/concurrent/NamedThreadFactory.h"
#include "../../folly/folly/wangle/channel/Handler.h"
#include "../../folly/folly/io/async/EventBaseManager.h"

namespace fractal {
    void ServerWorkerPool::threadStarted(
        folly::wangle::ThreadPoolExecutor::ThreadHandle* h) {
            auto worker = acceptorFactory_->newAcceptor(exec_->getEventBase(h));
            workers_.insert({h, worker});

            for (auto socket : *sockets_) {
                socket->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                    [this, worker, socket]() {
                        socketFactory_->addAcceptCB(
                            socket, worker.get(), worker->getEventBase());
                    });
            }
        }

    void ServerWorkerPool::threadStopped(
        folly::wangle::ThreadPoolExecutor::ThreadHandle* h) {
            auto worker = workers_.find(h);
            CHECK(worker != workers_.end());

            for (auto socket : *sockets_) {
                socket->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                    [&]() {
                        socketFactory_->removeAcceptCB(
                            socket, worker->second.get(), nullptr);
                    });
            }

            if (!worker->second->getEventBase()->isInEventBaseThread()) {
                worker->second->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                    [=]() {
                        worker->second->dropAllConnections();
                    });
            } else {
                worker->second->dropAllConnections();
            }

            workers_.erase(worker);
        }
}
