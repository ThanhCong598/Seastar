
#include <cmath>
#include "seastar/core/reactor.hh"
#include "seastar/core/app-template.hh"
#include "seastar/rpc/rpc.hh"
#include "seastar/core/sleep.hh"
#include "seastar/rpc/lz4_compressor.hh"
#include <seastar/core/distributed.hh>

#include <seastar/core/sstring.hh>
#include <map>
#include <iostream>

using namespace seastar;


class DataBase {

public: using TABLE_NAME =seastar::sstring;
        using  KEY=seastar::sstring;
        using VALUE=seastar::sstring;
        using TABLE=std::map<KEY,VALUE>;

private:
    std::map <TABLE_NAME, std::map<KEY,VALUE>> collection;
    TABLE FindTableByName(TABLE_NAME tableName){
        return collection.find(tableName)->second;
    };
    bool isTableExist(TABLE_NAME tableName){
        if(collection.find(tableName)!=collection.end()) return true;
        else return false;
    };
    bool isKeyExist(TABLE_NAME tableName,KEY key){
        auto it=collection.find(tableName);
        if(it==collection.end()) return false;
        else{
            auto table=it->second;
            if(table.find(key)==table.end()) return false;
        }
        return true;
    };
public:


    DataBase(const std::map <TABLE_NAME, std::map<KEY,VALUE>> collection):collection{collection}{};

    DataBase(){};

    seastar::future<> CreateTable(TABLE_NAME tableName){
        if (!isTableExist(tableName)) {
            TABLE newTable;
            collection.insert({tableName, newTable});
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_exception_future<>("404");
        }
    };
    seastar::future<> DropTable(TABLE_NAME tableName){
        if (isTableExist(tableName)) {
            collection.erase(tableName);
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_exception_future<>("404");
        }
    }
    seastar::future<> InsertByKey(TABLE_NAME tableName, KEY key, VALUE value){
        if (!isKeyExist(tableName, key)) {
            collection.find(tableName)->second.insert({key, value});
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_exception_future<>("404");
        }

    };
    seastar::future<> UpdateByKey(TABLE_NAME tableName, KEY key, VALUE value){
        if (isKeyExist(tableName, key)) {
            FindTableByName(tableName).at(key) = value;
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_exception_future<>("404");
        }
    };
    seastar::future<> DeleteByKey(TABLE_NAME tableName, KEY key){
        if (isKeyExist(tableName, key)) {
            FindTableByName(tableName).erase(key);
            return seastar::make_ready_future<>();
        } else {
            return seastar::make_exception_future<>("404");
        }
    };
    seastar::future<VALUE> Select(TABLE_NAME tableName,KEY key){
        if (isKeyExist(tableName, key)) {
            VALUE value = FindTableByName(tableName).find(key)->second;
            return seastar::make_ready_future<VALUE> (value);
        } else {
            return seastar::make_ready_future<VALUE > (to_sstring("Err"));
        }
    };



};


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

    std::cout << "start server ... "<<std::endl;
    rpc::protocol<serializer> myrpc(serializer{});
    static std::unique_ptr<rpc::protocol<serializer>::server> server;


    myrpc.set_logger([] (const sstring& log) {
        print("%s", log);
        std::cout << std::endl;
    });

    return app.run_deprecated(ac, av, [&] {

        // Run a storage in all core.
        auto storage = new seastar::distributed<DataBase>;
        storage->start().then([storage = std::move(storage)] () {
            seastar::engine().at_exit([storage] {
                return storage->stop();
            });
        }).then([] {
            std::cout << "Storage alive in all core " << " ...\n";
        });

        static mycomp mc;
        auto &&config = app.configuration();
        bool compress = false;



        // register handler

        myrpc.register_handler(21, [=](seastar::sstring tableName) {
            std::cout << "21: create table" << std::endl;
            return storage->invoke_on_all(&DataBase::CreateTable, tableName).then([=](){
                return make_ready_future<seastar::sstring>(to_sstring("Tao Bang Thanh Cong nha cha noi :))"));
            });
        });

        myrpc.register_handler(51, [&](seastar::sstring tableName) {
            std::cout << "51: Drop table" << std::endl;
            return storage->invoke_on_all(&DataBase::DropTable,tableName).then([=]{
                return make_ready_future<seastar::sstring>(to_sstring("Xoa Bang Thanh Cong nha cha noi :))"));
            });
        });

        myrpc.register_handler(22, [&](DataBase::TABLE_NAME tableName, DataBase::KEY key, DataBase::VALUE value) {
            std::cout << "22: Insert by key" << std::endl;
            return storage->invoke_on_all(&DataBase::InsertByKey,tableName,key,value).then([=]{
                return make_ready_future<seastar::sstring>(to_sstring("Them row Thanh Cong nha cha noi :))"));
            });

        });

        myrpc.register_handler(23, [&](DataBase::TABLE_NAME tableName, DataBase::KEY key, DataBase::VALUE value) {
            std::cout << "23: update by key" << std::endl;

            return storage->invoke_on_all(&DataBase::UpdateByKey,tableName,key,value).then([=]{
                return make_ready_future<seastar::sstring>(to_sstring("Update Thanh Cong nha cha noi :))"));
            });

        });
        myrpc.register_handler(24, [&](DataBase::TABLE_NAME tableName, DataBase::KEY key) {
            std::cout << "24: select" << std::endl;
             return storage->local_shared()->Select(tableName,key);
        });





        // config server
        rpc::resource_limits limits;
        limits.bloat_factor = 1;
        limits.basic_request_size = 0;
        limits.max_memory = 10'000'000;
        rpc::server_options so;
        u_int16_t port = 9000;
        if (compress) {
            so.compressor_factory = &mc;
        }
        std::cout << "server on port " << port << std::endl;
        server = std::make_unique<rpc::protocol<serializer>::server>(myrpc, so, ipv4_addr{("127.0.0.1", port)}, limits);

    });

}