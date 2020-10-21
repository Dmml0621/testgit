#include "CountQuery.h"
#include "../../db/Database.h"

constexpr const char *CountQuery::qname;

QueryResult::Ptr CountQuery::execute()
{
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    int cnt = 0;
    try {
        auto &table = db[this->targetTable];
        auto res = initCondition(table);
        if (res.second)
        {
            for (auto it : table)
            {
                if (this->evalCondition(it))
                    cnt++;
            }
        }
        return make_unique<SuccessMsgResult>(cnt);
    }
    catch (const TableNameNotFound &e){
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string CountQuery::toString()
{
    return "QUERY = COUNT " + this->targetTable + "\"";
}
