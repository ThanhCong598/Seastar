
#include <cmath>
#include "seastar/core/reactor.hh"
#include "seastar/core/app-template.hh"
#include "seastar/rpc/rpc.hh"
#include "seastar/core/sleep.hh"
#include "seastar/rpc/lz4_compressor.hh"



using namespace seastar;

struct serializer {
};

template <typename T, typename Output>
inline
void write_arithmetic_type(Output& out, T v) {
    static_assert(std::is_arithmetic<T>::value, "must be arithmetic type");
    return out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T, typename Input>
inline
T read_arithmetic_type(Input& in) {
    static_assert(std::is_arithmetic<T>::value, "must be arithmetic type");
    T v;
    in.read(reinterpret_cast<char*>(&v), sizeof(T));
    return v;
}

template <typename Output>
inline void write(serializer, Output& output, int32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, int64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, double v) { return write_arithmetic_type(output, v); }
template <typename Input>
inline int32_t read(serializer, Input& input, rpc::type<int32_t>) { return read_arithmetic_type<int32_t>(input); }
template <typename Input>
inline uint32_t read(serializer, Input& input, rpc::type<uint32_t>) { return read_arithmetic_type<uint32_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<uint64_t>) { return read_arithmetic_type<uint64_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<int64_t>) { return read_arithmetic_type<int64_t>(input); }
template <typename Input>
inline double read(serializer, Input& input, rpc::type<double>) { return read_arithmetic_type<double>(input); }

template <typename Output>
inline void write(serializer, Output& out, const sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
inline sstring read(serializer, Input& in, rpc::type<sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    sstring ret(sstring::initialized_later(), size);
    in.read(ret.begin(), size);
    return ret;
}

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

class mycomp : public rpc::compressor::factory {
    const sstring _name = "LZ4";
public:
    virtual const sstring& supported() const override {
        print("supported called\n");
        return _name;
    }
    virtual std::unique_ptr<rpc::compressor> negotiate(sstring feature, bool is_server) const override {
        print("negotiate called with %s\n", feature);
        return feature == _name ? std::make_unique<rpc::lz4_compressor>() : nullptr;
    }
};





int main(int ac, char** av) {

    app_template app;
    rpc::protocol<serializer> myrpc(serializer{});
    static std::unique_ptr<rpc::protocol<serializer>::client> client;

    myrpc.set_logger([] (const sstring& log) {
        print("%s", log);
        std::cout << std::endl;
    });

    return app.run_deprecated(ac, av, [&] {

        static mycomp mc;
        std::cout << "client" << std::endl;
        rpc::client_options co;
        co.compressor_factory = &mc;





        /// Register function from client
        auto CreateTable =myrpc.make_client<seastar::sstring (seastar::sstring table_name)>(21);
        auto DropTable =myrpc.make_client<seastar::sstring (seastar::sstring table_name)>(51);
        auto InsertData = myrpc.make_client<seastar::sstring (seastar::sstring table_name, seastar::sstring key, seastar::sstring value)>(22);
        auto UpdateData = myrpc.make_client<seastar::sstring (seastar::sstring table_name, seastar::sstring key, seastar::sstring value)>(23);
        auto Select = myrpc.make_client<seastar::sstring (seastar::sstring table_name, seastar::sstring key)>(24);


        client = std::make_unique<rpc::protocol<serializer>::client>(myrpc, co, ipv4_addr{ "127.0.0.1",9000});


        CreateTable(*client, to_sstring("tbl_1")).then([=](seastar::sstring result){
            std::cout <<result.c_str()<< std::endl;
        }).finally([](){

        });

        InsertData(*client, to_sstring("tbl_1"), to_sstring("key_1.1"), to_sstring("value_1.1")).then([=] (seastar::sstring result) {
            std::cout <<result.c_str()<< std::endl;
        }).finally([]{});

//        UpdateData(*client, to_sstring("tbl_1"), to_sstring("key_1.1"), to_sstring("updatevalue1.1")).then([client] () {
//            std::cout << "3.1: " <<  std::endl;
//        });
//
//
//        Select(*client, to_sstring("tbl_1"), to_sstring("key_1.1")).then([=] (seastar::sstring  value) {
//            std::cout <<value.c_str()<< std::endl;
//        });
//
//        DropTable(*client, to_sstring("tbl_1")).then([client] () {
//            std::cout << "3.1: "  << std::endl;
//        });



    });

}