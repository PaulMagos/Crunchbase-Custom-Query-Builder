import copy
import json
from itertools import product


# FLOWS
def payload_update(ed_loc, f_job_loc, s_job_loc, gender, job_start):
    return "{\"field_ids\":[\"identifier\",\"primary_job_title\",\"primary_organization\",\"rank_person\"],\"order\":[" \
           "{\"field_id\":\"rank_person\",\"sort\":\"asc\"}],\"query\":[{\"type\":\"sub_query\"," \
           "\"collection_id\":\"organization.has_alumni.reverse\",\"query\":[{\"type\":\"predicate\"," \
           "\"field_id\":\"location_identifiers\",\"operator_id\":\"includes\",\"include_nulls\":true,\"values\":[\"" \
           + ed_loc + "\"]}]},{\"type\":\"sub_query\",\"collection_id\":\"job.has_past_job.forward\"," \
                      "\"query\":[{\"type\":\"sub_query\"," \
                      "\"collection_id\":\"organization.has_past_position.reverse\",\"query\":[{" \
                      "\"type\":\"predicate\",\"field_id\":\"location_identifiers\"," \
                      "\"operator_id\":\"includes\",\"values\":[" \
                      "\"" + f_job_loc + "\"]}]},{\"type\":\"predicate\"," \
                                         "\"field_id\":\"ended_on\",\"operator_id\":\"between\",\"values\":[\"" + str(
        job_start) + "\"," \
                     "\"" + str(job_start + 1) + "\"]}]},{\"type\":\"sub_query\"," \
                                                 "\"collection_id\":\"job.has_past_job.forward\",\"query\":[{\"type\":\"sub_query\"," \
                                                 "\"collection_id\":\"organization.has_past_position.reverse\",\"query\":[{" \
                                                 "\"type\":\"predicate\",\"field_id\":\"location_identifiers\"," \
                                                 "\"operator_id\":\"includes\",\"values\":[" \
                                                 "\"" + s_job_loc + "\"]}]},{\"type\":\"predicate\"," \
                                                                    "\"field_id\":\"started_on\",\"operator_id\":\"between\",\"values\":[\"" + str(
        job_start) + "\"," \
                     "\"" + str(job_start + 1) + "\"]}]}],\"field_aggregators\":[]," \
                                                 "\"collection_id\":\"people\",\"limit\":1} "


# STOCKS
def payload_update_lite(ed_loc, f_job_loc, year, gender):
    return "{\"field_ids\":[\"identifier\",\"primary_job_title\",\"primary_organization\",\"location_identifiers\"," \
           "\"rank_person\"],\"order\":[{\"field_id\":\"rank_person\",\"sort\":\"asc\"}],\"query\":[{" \
           "\"type\":\"sub_query\",\"collection_id\":\"organization.has_alumni.reverse\",\"query\":[{" \
           "\"type\":\"predicate\",\"field_id\":\"location_identifiers\",\"operator_id\":\"includes\"," \
           "\"include_nulls\":null,\"values\":[\"" + ed_loc + "\"]}]},{\"type\":\"sub_query\"," \
                                                              "\"collection_id\":\"job.has_past_job.forward\"," \
                                                              "\"query\":[{\"type\":\"sub_query\"," \
                                                              "\"collection_id\":\"organization.has_past_position" \
                                                              ".reverse\",\"query\":[{\"type\":\"predicate\"," \
                                                              "\"field_id\":\"location_identifiers\"," \
                                                              "\"operator_id\":\"includes\",\"include_nulls\":null," \
                                                              "\"values\":[\"" + f_job_loc + "\"]}]}," \
                                                                                             "{\"type\":\"predicate" \
                                                                                             "\",\"field_id" \
                                                                                             "\":\"started_on\"," \
                                                                                             "\"operator_id\":\"lte" \
                                                                                             "\",\"include_nulls" \
                                                                                             "\":null,\"values\":[" \
                                                                                             "\"01/01/" + str(
        year) + "\"]},{\"type\":\"predicate\",\"field_id\":\"ended_on\",\"operator_id\":\"gte\"," \
                "\"include_nulls\":null,\"values\":[\"01/01/" + str(
        year) + "\"]}]},{\"type\":\"predicate\",\"field_id\":\"gender\",\"operator_id\":\"includes\"," \
                "\"include_nulls\":null,\"values\":[\"" + gender + "\"]}],\"field_aggregators\":[]," \
                                                                   "\"collection_id\":\"people\",\"limit\":15} "


def lite_query_needed(dataset, ed_country, work_country, year, gen):
    if len(dataset) != 0 and ed_country in dataset:
        if len(dataset[ed_country]) != 0 and work_country in dataset[ed_country]:
            if len(dataset[ed_country][work_country]) != 0 and year in dataset[ed_country][work_country]:
                if gen in dataset[ed_country][work_country][year]:
                    return False
    return True


def countries_get(my_file):
    # Opening JSON file
    with open(my_file + ".json", "r+") as f:
        data = json.load(f)
        # Closing file
        f.close()
        # returns JSON object as
        # a dictionary
        return data["Countries"]


def read_json(saved_country):
    try:
        fi = open(saved_country + ".json", "r+")
        new_dict = json.load(fi)
        fi.close()
    except:
        fi = open(saved_country + ".json", "w+")
        new_dict = {}
        json.dump(new_dict, fi, indent=4)
        fi.close()

    if len(new_dict) == 0:
        return {}
    return new_dict


def write_json(dataset, filename):
    with open(filename, "w+") as fi:
        json.dump(dataset, fi, indent=4)
        fi.close()


def queries_lite_create2():
    Countries = countries_get("SUBSET COUNTRIES")
    QUERIES = read_json("queries_lite2")
    combination = product(Countries, repeat=2)
    for combo in combination:
        if combo[0] not in QUERIES:
            QUERIES[combo[0]] = {}
        if combo[1] not in QUERIES[combo[0]]:
            QUERIES[combo[0]][combo[1]] = {}
        for YEAR in range(2010, 2022):
            QUERIES[combo[0]][combo[1]][str(YEAR)] = {}
            for gender in {"male", "female"}:
                payload = payload_update_lite(
                    Countries[combo[0]]["uuid"],
                    Countries[combo[1]]["uuid"],
                    YEAR,
                    gender
                )
                QUERIES[combo[0]][combo[1]][str(YEAR)][gender] = []
                QUERIES[combo[0]][combo[1]][str(YEAR)][gender].append(payload)
    write_json(QUERIES, "queries_lite2.json")


def get_query_lite(query_list, dataset="", query_file="queries_lite2"):
    queries = query_list
    if len(queries) == 0:
        queries = read_json(query_file)
    backup_queries = copy.deepcopy(queries)
    for ed_country in backup_queries:
        for work_country in backup_queries[ed_country]:
            for year in backup_queries[ed_country][work_country]:
                for gen in backup_queries[ed_country][work_country][year]:
                    if dataset != "":
                        if lite_query_needed(dataset, ed_country, work_country, year, gen):
                            return ed_country, work_country, year, gen, queries[ed_country][work_country][year].pop(
                                gen)[0], queries
                if len(queries[ed_country][work_country][year]) == 0 or year in dataset[ed_country][work_country]:
                    queries[ed_country][work_country].pop(year)
            if len(queries[ed_country][work_country]) == 0:
                queries[ed_country].pop(work_country)
        if len(queries[ed_country]) == 0:
            queries.pop(ed_country)

    return "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"
