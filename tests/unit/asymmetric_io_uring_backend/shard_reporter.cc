#include <seastar/core/app-template.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>
#include <sys/syscall.h>
#include <unistd.h>

using namespace seastar;

int main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, []() -> future<> {
        return seastar::async([&] {
            YAML::Emitter out;
            out << YAML::BeginDoc;
            out << YAML::BeginSeq;

            for (unsigned i = 0; i < smp::count; i++) {
                out << YAML::BeginMap;
                out << YAML::Key << "shard" << YAML::Value << i;
                smp::submit_to(i, [&out] () {
                    auto core_id = sched_getcpu();
                    out << YAML::Key << "cpu_id" << YAML::Value << core_id;
                    auto tid = static_cast<int>(syscall(SYS_gettid));
                    out << YAML::Key << "thread_id" << YAML::Value << tid;
                    return make_ready_future<>();
                }).get();
                out << YAML::EndMap;
            }

            out << YAML::EndSeq;
            out << YAML::EndDoc;
            std::cout << out.c_str();
        });
    });
}
