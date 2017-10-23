#pragma once

#include "../../folly/folly/wangle/bootstrap/ServerBootstrap-inl.h"
#include "../../folly/folly/Baton.h"
#include "../../folly/folly/wangle/channel/Pipeline.h"

namespace folly {
    typedef folly::wangle::Pipeline<folly::IOBufQueue&, std::unique_ptr<folly::IOBuf>> DefaultPipeline;

    template <typename Pipeline>
    class ServerBootstrap {
    public:

        ServerBootstrap(const ServerBootstrap& that) = delete;
        ServerBootstrap(ServerBootstrap&& that) = default;

        ServerBootstrap() {}

        ~ServerBootstrap() {
            stop();
            join();
        }

        typedef wangle::Pipeline<void*> AcceptPipeline;

        ServerBootstrap* pipeline(
            std::shared_ptr<PipelineFactory<AcceptPipeline>> factory) {
                pipeline_ = factory;
                return this;
            }
        )

        ServerBootstrap* channelFactory(
            std::shared_ptr<ServerSocketFactory> factory) {
                socketFactory_ = factory;
                return this;
            }
        )

        ServerBootstrap* childPipeline(
            std::shared_ptr<PipelineFactory<Pipeline>> factory) {
                childPipelineFactory_ = factory;
                return this;
            }
        )

        ServerBootstrap* group(
            std::shared_ptr<folly::wangle::IOThreadPoolExecutor> io_group) {
                return group(nullptr, io_group);
            }
        )

        ServerBootstrap* group(
            std::shared_ptr<folly::wangle::IOThreadPoolExecutor> accept_group,
            std::shared_ptr<wangle::IOThreadPoolExecutor> io_group) {
                if (!accept_group) {
                    accept_group = std::make_shared<folly::wangle::IOThreadPoolExecutor>(
                        1, std::make_shared<wangle::NamedThreadFactory>("Acceptor Thread"));
                    )
                }
                if (!io_group) {
                    io_group = std::make_shared<folly::wangle::IOThreadPoolExecutor>(
                        32, std::make_shared<wangle::NamedThreadFactory>("IO Thread"));
                    )
                }

                CHECK(!(acceptorFactory_ && childPipelineFactory_));

                if (acceptorFactory_) {
                    workerFactory_ = std::make_shared<ServerWorkerPool>(
                        acceptorFactory_, io_group.get(), sockets_, socketFactory_);
                    )
                } else {
                    workerFactory_ = std::make_shared<ServerWorkerPool>(
                        std::make_shared<ServerAcceptorFactory<Pipeline>>(
                            childPipelineFactory_,
                            pipeline_),
                        io_group.get(), sockets_, socketFactory_);
                }

                io_group->addObserver(workerFactory_);

                acceptor_group_ = accept_group;
                io_group_ = io_group;


                return this;
            }

            void bind(folly::AsyncServerSocket::UniquePtr s) {
                if (!workerFactory_) {
                    group(nullptr);
                }

                CHECK(acceptor_group_->numThreads() == 1);

                std::shared_ptr<folly::AsyncServerSocket> socket(
                    s.release(), DelayedDestruction::Destructor());

                folly::Baton<> barrier;
                acceptor_group_->add([&]() {
                   scoket->attachEventBase(EventBaseManager::get()->getEventBase());
                   socket->listen(socketConfig.acceptBacklog);
                   socket->startAccepting();
                   barrier.post(); 
                });
                barrier.wait();

                // start all threads
                workerFactory_->forEachWorker([this, socket](Acceptor* worker) {
                    socket->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                        [this, worker, socket]() {
                            socketFactory_->addAcceptCB(socket, worker, worker->getEventBase());
                        });
                });
                
                sockets_->push_back(socket);
            }

        void bind(folly::SocketAddress& address) {
            bindImpl(-1, address);
        }

        void bind(int port) {
            CHECK(port >= 0);
            folly::SocketAddress address;
            bindImpl(port, address);
        }

        void bindImpl(int port, folly::SocketAddress& address) {
            if (!workerFactory_) {
                group(nullptr);
            }

            bool reusePort = false;
            if (acceptor_group_->numThreads() > 1) {
                reusePort = true;
            }

            std::mutex sock_lock;
            std::vector<std::shared_ptr<folly::AsyncSocketBase>> new_sockets;

            std::exception_ptr exn;

            auto startupFunc = [&](std::shared_ptr<folly::Baton<>> barrier) {
                try {
                    auto socket = socketFactory_->newSocket(
                        port, address, socketConfig.acceptBacklog, reusePort, socketConfig);

                    sock_lock.lock();
                    new_sockets.push_back(socket);
                    sock_lock.unlock();

                    if (port <= 0) {
                        socket->getAddress(&address);
                        port = address.getPort();
                    }

                    barrier->post();
                } catch (...) {
                    exn = std::current_exception();
                    barrier->post();

                    return;
                }
            };

            auto wait0 = std::make_shared<folly::Baton<>>();
            acceptor_group_->add(std::bind(startupFunc, wait0));
            wait0->wait();

            for (size_t i = 1; i < acceptor_group_->numThreads(); i++) {
                auto barrier = std::make_shared<folly::Baton<>>();
                acceptor_group_->add(std::bind(startupFunc, barrier));
                barrier->wait();
            }
            
            if (exn) {
                std::rethrow_exception(exn);
            }

            for (auto& socket : new_sockets) {
                // start all threads
                workerFactory_->forEachWorker([this, socket](Acceptor* worker) {
                    socket->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                        [this, worker, socket]() {
                            socketFactory_->addAcceptCB(socket, worker, worker->getEventBase());
                        });
                });

                sockets_->push_back(socket);
            }
        }

        // stop listening on all sockets
        void stop() {
            if (sockets_) {
                for (auto socket : *sockets_) {
                    socket->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
                        [&]() mutable {
                            socketFactory_->stopSocket(socket);
                        });
                }
                sockets_->clear();
            }

            if (!stopped_) {
                stopped_ = true;
                if (stopBaton_) {
                    stopBaton_->post();
                }
            }
        }

        void join() {
            if (acceptor_group_) {
                acceptor_group_->join();
            }
            if (io_group_) {
                io_group_->join();
            }
        }

        void waitForStop() {
            if (!stopped_) {
                CHECK(stopBaton_);
                stopBaton_->wait();
            }
        }

        const std::vector<std::shared_ptr<folly::AsyncSocketBase>>&
        getSockets() const {
            return *sockets_;
        }

        std::shared_ptr<wangle::IOThreadPoolExecutor> getIOGroup() const {
            return io_group_;
        }

        template <typename F>
        void forEachWorker(F&& f) const {
            workerFactory_->forEachWorker(f);
        }

        ServerSocketConfig socketConfig;

    private:
        std::shared_ptr<wangle::IOThreadPoolExecutor> acceptor_group_;
        std::shared_ptr<wangle::IOThreadPoolExecutor> io_group_;

        std::shared_ptr<ServerWorkerPool> workerFactory_;
        std::shared_ptr<std::vector<std::shared_ptr<folly::AsyncSocketBase>>> socket_{
            std::make_shared<std::vector<std::shared_ptr<folly::AsyncSocketBase>>>()
        };

        std::shared_ptr<AcceptorFactory> acceptorFactory_;
        std::shared_ptr<PipelineFactory<Pipeline>> childPipelineFactory_;
        std::shared_ptr<PipelineFactory<AcceptPipeline>> pipeline_{
            std::make_shared<DefaultAcceptPipelineFactory>()
        };
        std::shared_ptr<ServerSocketFactory> socketFactory_{
            std::make_shared<AsyncServerSocket>()
        };

        std::unique_ptr<folly::Baton<>> stopBaton_{
            folly::make_unique<folly::Baton<>>()
        };
    }
}