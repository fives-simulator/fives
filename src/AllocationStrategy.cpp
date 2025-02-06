#include "AllocationStrategy.h"

#include <cstdint>
#include <random>

WRENCH_LOG_CATEGORY(fives_allocator, "Log category for Fives Allocators");

namespace fives {

    std::vector<std::shared_ptr<wrench::FileLocation>>
    GenericRRAllocator::operator()(
        const std::shared_ptr<wrench::DataFile> &file,
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources,
        const std::map<std::shared_ptr<wrench::DataFile>,
                       std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>
            &previous_allocations,
        unsigned int stripe_count) {

        // Init round-robin
        static auto last_selected_server = resources.begin()->first;
        static unsigned int internal_disk_selection = 0;
        auto capacity_req = file->getSize();
        std::shared_ptr<wrench::FileLocation> designated_location = nullptr;
        auto current = resources.find(last_selected_server);
        auto current_disk_selection = internal_disk_selection;

        auto continue_disk_loop = true;

        do {

            unsigned int nb_of_local_disks = current->second.size();
            auto storage_service =
                current->second[current_disk_selection % nb_of_local_disks];

            auto free_space = storage_service->getTotalFreeSpace();

            if (free_space >= capacity_req) {
                designated_location = wrench::FileLocation::LOCATION(
                    std::shared_ptr<wrench::StorageService>(storage_service), file);
                std::advance(current, 1);
                if (current == resources.end()) {
                    current = resources.begin();
                    current_disk_selection++;
                }
                // Update for next function call
                last_selected_server = current->first;
                internal_disk_selection = current_disk_selection;
                break;
            }

            std::advance(current, 1);
            if (current == resources.end()) {
                current = resources.begin();
                current_disk_selection++;
            }
            if (current_disk_selection >
                (internal_disk_selection + nb_of_local_disks + 1)) {
                continue_disk_loop = false;
            }
        } while ((current->first != last_selected_server) or (continue_disk_loop));

        if (designated_location)
            return std::vector<std::shared_ptr<wrench::FileLocation>>{
                designated_location};
        else
            return std::vector<std::shared_ptr<wrench::FileLocation>>();
    }

    // --------------------------- LUSTRE

    std::vector<std::shared_ptr<wrench::FileLocation>>
    fives::LustreAllocator::operator()(
        const std::shared_ptr<wrench::DataFile> &file,
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources,
        const std::map<std::shared_ptr<wrench::DataFile>,
                       std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>
            &previous_allocations,
        unsigned int stripe_count) {

        // Check for inbalance in the storage services utilization
        auto ba_min_max = this->lustreComputeMinMaxUtilization(resources);

        if (this->lustreUseRR(ba_min_max)) {
            // Consider that every target has roughly the same free space, and use RR
            // allocator
            WRENCH_DEBUG("[LustreAllocator] Using RR allocator");
            return this->lustreRRAllocator(file, resources, mapping,
                                           previous_allocations, stripe_count);
        } else {
            // Consider that targets free space use is too much imbalanced and go for
            // the weighted allocator
            WRENCH_DEBUG("[LustreAllocator] Using WEIGHTED allocator");
            return this->lustreWeightedAllocator(file, resources, mapping,
                                                 previous_allocations, stripe_count);
        }
    }

    ba_min_max LustreAllocator::lustreComputeMinMaxUtilization(
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources) const {

        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L488

        ba_min_max ba_min_max = {UINT64_MAX, 0};

        for (const auto &resource : resources) {
            for (const auto &service : resource.second) {
                auto ss_service =
                    dynamic_pointer_cast<wrench::SimpleStorageService>(service);
                uint64_t current_free_space =
                    ss_service->getTotalFreeSpaceZeroTime(); // TODO : should this be
                                                             // ZeroTime or not ?
                WRENCH_DEBUG("[lustreComputeMinMaxUtilization] Cur. free space [%s]: %ld "
                             "(>>8 = %ld)",
                             ss_service->getName().c_str(), current_free_space,
                             current_free_space >> 8);
                current_free_space >>= 8; // used in Lustre code to prevent overflows,
                                          // we're blindly doing the same
                ba_min_max.min = min(current_free_space, ba_min_max.min);
                ba_min_max.max = max(current_free_space, ba_min_max.max);
            }
        }

        return ba_min_max;
    }

    bool LustreAllocator::lustreUseRR(struct ba_min_max ba_min_max) const {
        // Unlike in Lustre, we select the allocator based on avail bytes only, and
        // not avail byte + avail inodes This is because avail inodes is less relevant
        // for us (no MDT model)
        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L587

        // Also note : >> 8 is equivalent to "/ 256" only more optimised, so this is
        // equivelent to computing : "is 83 % of 'max' greater than min" (Thus leading
        // to the determining if the imbalance between most and least used OST is
        // greater than 17%)

        // Note : if your OSTs originally have != capacities, this will most likely
        // not work as intended (but Lustre has other mechanisms to look into for such
        // use cases)
        WRENCH_DEBUG("[lustreUseRR] Min : %lu / Max : %lu", ba_min_max.min,
                     ba_min_max.max);
        return (ba_min_max.max * (256 - this->config->lustre.lq_threshold_rr)) >> 8 <
               ba_min_max.min;
    }

    /**
     * @brief Order services in the same fashion Lustre allocator orders OSTs from
     * differents OSSs This function is the equivalent as lod_qos_calc_rr() in
     * 'lustre/lod/lod_qos.c'. The result in C++ looks slightly unnatural, but
     * follows the same broad logic as the original C code:
     *        - Create an empty list of OSTs, all values being null (LOV_QOS_EMPTY
     * in Lustre, a simple nullptr in our case), we call it rr_ordered_services
     *        - Loop over all OSS/servers (storage nodes hostnames in our case)
     *        - For each OSS/server:
     *           - loop for i=0 to the number of OSTs that will be in the final
     * ordered list
     *           - for each i, check if the tgt at position i in the pool of targets
     * is available, if not, continue (we don't check for that all our targets are
     * supposed alive at all time)
     *           - then check whether or not the identified target belongs to the
     * current server (upper loop), if not, continue
     *           - then compute where the current target should be placed in the
     * ordered list : this is done with "j * rr_ordered_services.size() / number of
     * OSTs in current server", j being a 0-based index for each server
     *           - loop over the entire pool of of OSTs being ordered
     *
     *
     * @param resources Reference to the resource map provided to the allocator
     * (vector of services, aka OSTs, for each storage node, aka OSS)
     * @return List of ordered services
     */
    std::vector<std::shared_ptr<wrench::StorageService>>
    LustreAllocator::lustreRROrderServices(
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources) const {

        if (resources.empty()) {
            throw std::runtime_error("[lustreRROrderService] Cannot return ordering "
                                     "from empty map of services");
        }

        std::vector<std::shared_ptr<wrench::StorageService>>
            disk_level_services;             // OSTs
        for (const auto &host : resources) { // Flatten OST list
            disk_level_services.insert(disk_level_services.end(), host.second.begin(),
                                       host.second.end());
            WRENCH_DEBUG("[lustreRROrderServices] Node %s has %lu service(s) (OST(s))",
                         host.first.c_str(), host.second.size());
        }

        uint32_t number_of_services = disk_level_services.size();
        uint32_t placed_services = 0;
        std::vector<std::shared_ptr<wrench::StorageService>> rr_ordered_services(
            number_of_services, nullptr);

        // Outer loop is at OSS/storage node level
        for (const auto &server : resources) {

            auto j = 0;
            auto srv_hostname = server.first;
            auto srv_service_count = server.second.size();

            // Here, the C code loops from 0 to lqr->lqr_pool.op_count, which is the
            // number of targets/OSTs in the final ordered list of targets (the one
            // being updated)
            for (std::size_t i = 0; i < number_of_services; i++) {
                int next;

                // Here, the C code checks whether or not the i-th target is available or
                // not We consider the services are always available

                // If the current service doesn't belong to the currently selected server,
                // skip it
                if (disk_level_services[i]->getHostname() != srv_hostname) {
                    continue;
                }

                // Compute placement index for the i-th service currently selected
                next = j * number_of_services / srv_service_count;
                // Find first empty slot in the vector, starting after 'next' index (and
                // loop back to the start of the vector if needed)
                while (rr_ordered_services[next] != nullptr) {
                    next = (next + 1) % number_of_services;
                }
                // Actual placement
                rr_ordered_services[next] = disk_level_services[i];
                j++;
                placed_services++;
            }
        }

        if (placed_services != rr_ordered_services.size()) {
            WRENCH_WARN("[lustreRROrderServices] Couldn't place all services in the "
                        "round-robin pre-processed list (missing %lu services)",
                        rr_ordered_services.size() - placed_services);
            throw std::runtime_error("Number of placed services differs expected size "
                                     "of the RR-ordered services");
        }

        return rr_ordered_services;
    }

    /**
     * @brief This method selects appropriate stripe_size and stripe_count values
     * for each file we want to write, depending on the file size and number of OSTs
     * available. In real life, the user would either rely on system defaults or
     * manually provide these parameters. This second option is not available to us
     * (we don't have the data in our datasets and we don't want this kind of user
     * interaction anyway)
     *
     *        As a reminder:
     *          - stripe_count is the number of OSTs used to stored chunks/stripes
     * of a file. For instance, if the stripe_count is set to '2', Lustre would
     * balance data blocks for a given files alternatively between 2 OSTs)
     *          - stripe_size is the size of a single stripe
     *          - there is also a concept of 'stripes_per_ost', but it is only used
     * with a special feature of Lustre, "overstriping", which we don't handle yet,
     * so it's always set to 1 in our case.
     *
     *        By default, we use the values provided in the configuration file
     * (lustre.stripe_count & lustre.stripe_size), BUT if these values result in too
     * many chunks for a given file (typically in case of very large IO operations),
     * we increase the stripe_size to reduce the number of operations on file and
     * speed up the simulation.
     *
     * @param file_size_b Size in bytes of the file which will be striped across the
     * storage resources
     * @param total_number_of_osts Total number of OSTs (StorageServices) available
     * in our storage system
     * @return Final validated striping parameters
     */
    striping
    LustreAllocator::lustreComputeStriping(uint64_t file_size_b,
                                           size_t total_number_of_OSTs,
                                           unsigned int stripe_count) const {

        WRENCH_DEBUG("[lustreComputeStriping] file size : %ld bytes, number of "
                     "available OSTs : %lu",
                     file_size_b, total_number_of_OSTs);

        striping ret_striping = {};
        ret_striping.stripe_size_b = this->config->lustre.stripe_size;

        if (stripe_count != 0) {
            ret_striping.stripes_count = stripe_count;
        } else {
            ret_striping.stripes_count = this->config->lustre.stripe_count;
        }

        if (file_size_b < 1) {
            WRENCH_WARN("[lustreComputeStriping] File size can't be < 1B");
            throw std::runtime_error("File size can't be < 1B");
        }

        if (total_number_of_OSTs < 1) {
            WRENCH_WARN("[lustreComputeStriping] Total number of OST can't be < 1");
            throw std::runtime_error("Total number of OST can't be < 1");
        }

        if (file_size_b <= this->config->lustre.stripe_size) {
            WRENCH_INFO("[lustreComputeStriping] File size <= stripe_size, there will "
                        "be only one stripe on one OST");
            ret_striping.stripes_count = 1;       // not possible to create more than one
                                                  // stripe, so we can't use more than one OST
            ret_striping.max_stripes_per_ost = 1; // only one stripe on the selected OST
            return ret_striping;
        }

        if (ret_striping.stripes_count > total_number_of_OSTs) {
            WRENCH_WARN("[lustreComputeStriping] Configuration lustre.stripe_count "
                        "(%lu) is superior to the actual number of OSTs accessible by "
                        "the allocator (%lu)",
                        ret_striping.stripes_count, total_number_of_OSTs);
            throw std::runtime_error("The configured 'stripe_count' is higher than the "
                                     "actual number of available OSTs");
        }

        // Considering the default stripe_size and file size, how many file chunks
        // would be used in the striping pattern?
        auto nb_chunks =
            std::ceil(static_cast<double>(file_size_b) / ret_striping.stripe_size_b);
        WRENCH_DEBUG("[lustreComputeStriping] Number of chunks (before "
                     "max_chunks_per_ost check): %f",
                     nb_chunks);
        // Considering the number of chunks at this point, and the default number of
        // OSTs to use, how many chunks would end up on each OST? uint64_t
        // stripes_per_ost = std::ceil(nb_chunks / ret_striping.stripes_count);
        ret_striping.max_stripes_per_ost =
            std::ceil(static_cast<double>(nb_chunks) / ret_striping.stripes_count);

        // If we have too many chunks on each OST, recompute the stripe_size to fit
        // the user defined bounds Note: the stripe_count is not updated, we still
        // want to use the same number of OSTs in total, only the stripe_size
        if (ret_striping.max_stripes_per_ost >
            this->config->lustre.max_chunks_per_ost) {
            ret_striping.stripe_size_b = std::ceil(
                static_cast<double>(file_size_b) /
                (ret_striping.stripes_count * config->lustre.max_chunks_per_ost));
            WRENCH_WARN("[lustreComputeStriping] Too many stripes per ost with "
                        "configured stripe_size - recomputing to %lu",
                        ret_striping.stripe_size_b);
            // nb_chunks = std::ceil(static_cast<double>(file_size_b) /
            // ret_striping.stripe_size_b); ret_striping.max_stripes_per_ost =
            // std::ceil(nb_chunks / ret_striping.stripes_count);
            ret_striping.max_stripes_per_ost = this->config->lustre.max_chunks_per_ost;
        }

        WRENCH_DEBUG("[lustreComputeStriping] stripe_count = %lu ; stripe_size = %lu "
                     "; [stripes_per_ost = %lu]",
                     ret_striping.stripes_count, ret_striping.stripe_size_b,
                     ret_striping.max_stripes_per_ost);

        return ret_striping;
    }

    bool LustreAllocator::lustreOstIsUsed(
        const std::map<std::shared_ptr<wrench::DataFile>,
                       std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
        const std::shared_ptr<wrench::StorageService> ost) const {

        for (const auto &alloc_mapping : mapping) {
            auto location_vector = alloc_mapping.second;
            for (const auto &location : location_vector) {
                if (location->getStorageService()->getHostname() == ost->getHostname()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @brief Homemade helper to actually distribute file parts on the selected
     * OSTs,
     */
    std::vector<std::shared_ptr<wrench::FileLocation>>
    LustreAllocator::lustreCreateFileParts(
        const std::shared_ptr<wrench::DataFile> &file,
        std::vector<std::shared_ptr<wrench::StorageService>> selectedOSTs,
        uint64_t stripeSize) const {

#if 1
        WRENCH_DEBUG("[lustreCreateFileParts] Creating parts of file %s on %lu != "
                     "OSTs, with stripe_size=%lu",
                     file->getID().c_str(), selectedOSTs.size(), stripeSize);
        // Actually create file_parts load-balanced on selected OSTs
        std::vector<std::shared_ptr<wrench::FileLocation>> designated_locations = {};
        uint64_t file_size_b = file->getSize();
        uint64_t allocated = 0;
        uint64_t chunk_idx = 0;
        auto service = selectedOSTs.begin();
        while (allocated < file_size_b) {
            auto part = wrench::Simulation::addFile(
                file->getID() + "_stripe_" + std::to_string(chunk_idx), stripeSize);
            // std::cout << "     - Creating file part " << file->getID() << "_" <<
            // std::to_string(chunk_idx) << " on " << service->get()->getName() <<
            // std::endl;
            designated_locations.push_back(wrench::FileLocation::LOCATION(
                std::shared_ptr<wrench::StorageService>(*service), part));

            allocated += stripeSize;
            chunk_idx++;
            if ((service != selectedOSTs.end()) &&
                (next(service) == selectedOSTs.end())) {
                service = selectedOSTs.begin();
            } else {
                service++;
            }
        }
#endif
#if 0
        // Actually create file_parts load-balanced on selected OSTs
        std::vector<std::shared_ptr<wrench::FileLocation>> designated_locations = {};
        uint64_t file_size_b = file->getSize();
        uint64_t allocated = 0;
        uint64_t chunk_idx = 0;
        auto service = selectedOSTs.begin();

        auto nb_services = selectedOSTs.size();
        auto meta_stripe_size = file_size_b / nb_services;

        WRENCH_DEBUG("[lustreCreateFileParts] Creating parts of file %s on %lu != OSTs, with stripe_size=%lu",
                     file->getID().c_str(), selectedOSTs.size(), meta_stripe_size);

        for (const auto &servive : selectedOSTs) {
            auto part = wrench::Simulation::addFile(file->getID() + "_metastripe_" + std::to_string(chunk_idx), meta_stripe_size);
            // std::cout << "     - Creating file part " << file->getID() << "_" << std::to_string(chunk_idx) << " on " << service->get()->getName() << std::endl;
            designated_locations.push_back(
                wrench::FileLocation::LOCATION(
                    std::shared_ptr<wrench::StorageService>(*service), part));

            allocated += meta_stripe_size;
            chunk_idx++;
        }

#endif
        WRENCH_DEBUG("[lustreCreateFileParts] %lu file parts have been created",
                     designated_locations.size());

        return designated_locations;
    }

    /**
     * @brief Lustre Round-Robin allocator. Implemented after the original C sources
     * from the Lustre project (https://github.com/whamcloud/lustre), with
     * simplifications and adaptations to fit our own data structures and
     * parameters.
     *
     * @param file The file we need to stripe and allocate on the storage resources
     * @param resources Map of resoureces representing the OSSs and OSTs available
     * to the allocator (in Wrench, hostnames of storage nodes and StorageServices
     * set up on them)
     * @param mapping Map of current files allocated to the resources, with a list
     * of the locations of all file parts for each file (used mainly to avoid
     * overloaded StorageServices)
     * @param previous_allocations Not used by the LustreAllocator, this is a list
     * the previous allocations for the other parts of a file, in case the allocator
     * is stateless and the CSS handled file striping.
     *
     */
    std::vector<std::shared_ptr<wrench::FileLocation>>
    LustreAllocator::lustreRRAllocator(
        const std::shared_ptr<wrench::DataFile> &file,
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources,
        const std::map<std::shared_ptr<wrench::DataFile>,
                       std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>
            &previous_allocations,
        unsigned int stripe_count) {

        /** 1. Prepare an ordered list of disk-level services (~OST) from all
         * resources (~OSS) In Lustre, this operation is systematically triggered
         * because the number of available targets may vary from one allocation to
         * another (hosts going offline, ...), but in our case, we consider that all
         * storages services acting as OSTs will always be up and running during the
         * simulation, so the ordered service list never changes
         */
        if (this->static_rr_ordered_services.empty()) {
            this->static_rr_ordered_services = lustreRROrderServices(resources);
            WRENCH_DEBUG("[lustreRRAllocator] Static list of ordered services has been "
                         "updated. It now contains %lu services",
                         static_rr_ordered_services.size());
        }
        uint32_t rr_service_count = static_rr_ordered_services.size();

        /** 1.Bis. Choose an appropriate striping strategy for the current allocation
         *  In Lustre, the user provides the striping pattern with the stripe_size and
         * stripe_count (number of OSTs onto which the stripes will be placed)
         *  parameters.
         *  Here we don't have (nor want) this kind of user input, so the simulator's
         * configuration provides a fixed stripe_size and stripe_count for all jobs,
         * and we possibly adapt it to stay within reasonnable bounds in terms of
         * simulation time.
         *
         *  We also compute a conservativeFreeSpaceRequirement, based on the stripe
         * size and the rounded-up number of stripes per OST. This is not directly
         * part of Lustre (not in this piece of code at least), but allows us to
         * exclude OSTs which would not be able to fit the file parts allocations
         * later on (let's acknowledge that during 'normal' Lustre lifecycle, a single
         * allocation is probably not expected to fill up a whole OST very often, but
         * who knows)
         */
        WRENCH_DEBUG("[lustreAllocator] Computing striping for file %s",
                     file->getID().c_str());
        auto current_striping =
            lustreComputeStriping(file->getSize(), rr_service_count, stripe_count);
        uint32_t conservativeFreeSpaceRequirement =
            current_striping.stripe_size_b * current_striping.max_stripes_per_ost;

        /** 2. Use the rr ordered list of services to allocation stripes for the given
         * file. Roughly the equivalent of lod_qos.c::lod_ost_alloc_rr() and
         * lod_qos.c::lod_check_and_reserve_ost()
         *
         *  Main steps are:
         *   - Gather all necessary informations (that's what we did with previous
         * function calls) : stripe_count, OST pool, etc
         *   - Update the starting index: i.e. the index of the first OST to be used
         * for placing stripes. This is a bit complex. Lustre stores a start_count
         * counter in its rr QoS data structure, acting as a 'reseed counter'. When it
         * reaches 0, Lustre reseeds the start index with a random value between 0 and
         * the total number of OSTs. The rest of the time, the start index is simply
         * either not updated or set to start_index %= number of OSTs, and it's
         * incremented during the allocation process afterward
         *   - If LOV_PATTERN_OVERSTRIPING is set (see overstripe here:
         * https://doc.lustre.org/lustre_manual.xhtml#file_striping.how_it_works),
         *     compute "stripes_per_ost' (we don't do this so far, stripes_per_osts =
         * 1 in all cases)
         *   - Loop starting at i=0, with condition i < "number of OSTs *
         * stripes_per_ost" && stripe_idx < stripe_count, where:
         *     - the first part simply equals to the total number of OSTs
         *     - the second part means we stop when we have allocated the last stripe
         * (stripe_idx starts at 0)
         *       - For each iteration, compute an index in the OST array from the
         * start_index, and choose the corresponding OST
         *       - Run a few preliminary checks (we skip a few which don't easily
         * apply to us, eg: is the target 'active'? not read-only? not degraded? a
         * 'composite' layout?)
         *       - Try to avoid OSTs if they are already part of the striping (when
         * multiple iterations over OSTs are needed?)
         *       - Actually select the OST for a stripe of the current allocation
         *   - When exiting the loop, check whether or not all stripes have indeed
         * been placed. If not, we can retry once with more relaxed selection of OSTs
         *
         */

        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/lod/lod_qos.c#L771
        if (--this->start_count <= 0) {
            std::random_device rd;
            std::mt19937 gen(rd()); // replace rd() by a static seed for reproducibility
            std::uniform_int_distribution<> distrib(0, rr_service_count - 1);
            this->start_ost_index = distrib(
                gen); // similar to when Lustre chooses the first OST to be used.
            // The actual line below in Lustre is : '(LOV_CREATE_RESEED_MIN /
            // max(osts->op_count, 1U) + LOV_CREATE_RESEED_MULT) * max(osts->op_count,
            // 1U);' with LOV_CREATE_RESEED_MIN = 2000 and LOV_CREATE_RESEED_MULT = 30
            // ...
            this->start_count =
                (2000 / rr_service_count + 30) *
                rr_service_count; // Raaaaaaandomm (but taken from Lustre sources)
        } else if (this->start_ost_index > rr_service_count ||
                   current_striping.stripes_count >= rr_service_count) {
            this->start_ost_index %= rr_service_count;
        }
        unsigned int temp_start_ost_index = start_ost_index;
        unsigned int stripe_idx = 0;

        std::vector<std::shared_ptr<wrench::StorageService>> ostSelection;

        // Some sort of retry counter (same name as in Lustre but I don't know why
        // they call it 'speed')
        for (auto speed = 0; speed < 2;
             speed++) { // IN the C code, this is implemented with a GOTO, not a loop
            for (size_t i = 0;
                 i < rr_service_count && stripe_idx < current_striping.stripes_count;
                 i++) {
                auto array_idx =
                    start_ost_index %
                    rr_service_count; // there should be an additional offset index here,
                                      // we simplified the algorithm a bit
                ++start_ost_index;
                auto current_ost = this->static_rr_ordered_services[array_idx];

                // /!\ Should (could) this be a call to getTotalFreeSpace() instead ?
                if (current_ost->getTotalFreeSpaceZeroTime() <
                        conservativeFreeSpaceRequirement or
                    (current_ost->getState() != wrench::S4U_Daemon::State::UP)) {
                    // Only keep running storage services associated with non-full disks
                    WRENCH_DEBUG("[lustreRRAllocator] Skipping OST (total free space == "
                                 "%lld and current state == %d)",
                                 current_ost->getTotalFreeSpaceZeroTime(),
                                 current_ost->getState());
                    continue;
                }

                // For the first iteration, avoid services which already have an older
                // allocation on them.
                if (speed == 0 && lustreOstIsUsed(mapping, current_ost)) {
                    WRENCH_DEBUG("[lustreRRAllocator] Skipping OST in first try because "
                                 "another allocation is already using it");
                    continue;
                } else {
                    WRENCH_DEBUG("[lustreRRAllocator] Using OST %s -> %s",
                                 current_ost->getHostname().c_str(),
                                 current_ost->getName().c_str());
                    ostSelection.push_back(current_ost);
                    stripe_idx++;
                }
            }

            if (stripe_idx == current_striping.stripes_count) {
                WRENCH_DEBUG("[lustreRRAllocator] All stripes allocated/ Stopping");
                break;
            } else {
                this->start_ost_index =
                    temp_start_ost_index; // back to original index, but this time we'll
                                          // allow 'slow' OSTs
                WRENCH_DEBUG("[lustreRRAllocator] Starting over because stripe_idx != "
                             "stripe_count and we reached the condition of this loop");
            }
        }

        // If we could allocate every stripe, return an empty vector that will be
        // interpreted as an allocation failure
        if (ostSelection.size() < current_striping.stripes_count) {
            WRENCH_WARN("[lustreRRAllocator] Expected to find %li OSTs, but could only "
                        "select %li instead.",
                        current_striping.stripes_count, ostSelection.size());
        }

        return lustreCreateFileParts(file, ostSelection,
                                     current_striping.stripe_size_b);
    }

    uint64_t
    LustreAllocator::lustreComputeOstPenalty(uint64_t free_space_b,
                                             uint64_t used_inode_count,
                                             double active_service_count) const {

        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L532

        if (active_service_count == 0) {
            WRENCH_WARN(
                "[lustreComputeOstPenalty] active_service_count = 0 - Unable to "
                "continue (check the resource map provided to the allocator))");
            throw std::runtime_error(
                "lustreComputeOstPenalty: active_service_count cannot be = 0");
        }

        free_space_b >>= 16; // Bitshift for overflow
        uint64_t free_inode_count =
            (this->config->lustre.max_inodes - used_inode_count);
        free_inode_count >>= 8;
        uint64_t penalty = ((this->prio_wide * free_space_b * free_inode_count) >> 8);
        penalty /= active_service_count;
        penalty >>= 1;
        return penalty;
    }

    uint64_t LustreAllocator::lustreComputeOstWeight(uint64_t free_space_b,
                                                     uint64_t used_inode_count,
                                                     uint64_t ost_penalty) const {

        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L232

        uint64_t weight = (free_space_b >> 16) *
                          ((this->config->lustre.max_inodes - used_inode_count) >> 8);

        if (weight < ost_penalty)
            return 0;
        else
            return (weight - ost_penalty);
    }

    uint64_t LustreAllocator::lustreComputeOssPenalty(uint64_t free_space_b,
                                                      uint64_t free_inode_count,
                                                      size_t ost_count,
                                                      size_t oss_count) const {

        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/obdclass/lu_tgt_descs.c#L563
        /** Per server penalty:
         *  prio * current__srv_free_space * current_srv_free_inodes / number of
         * targets in server / (number of server - 1) / 2
         */

        if (ost_count == 0 or oss_count == 0) {
            WRENCH_WARN(
                "[lustreComputeOstPenalty] oss_count and ost_count cannot be = 0");
            throw std::runtime_error(
                "lustreComputeOstPenalty: oss_count and ost_count cannot be = 0");
        }

        auto oss_penalty = (this->prio_wide * free_space_b * free_inode_count) >> 8;
        oss_penalty /= (ost_count * oss_count);
        oss_penalty >>= 1;
        return oss_penalty;
    }

    /**
     * @brief Lustre-inspired allocation algorithm (round-robin with a few tweaks)
     *        meant to be used with the INTERNAL_STRIPING property of the CSS set to
     * false (striping is done here).
     *
     */
    std::vector<std::shared_ptr<wrench::FileLocation>>
    LustreAllocator::lustreWeightedAllocator(
        const std::shared_ptr<wrench::DataFile> &file,
        const std::map<std::string,
                       std::vector<std::shared_ptr<wrench::StorageService>>>
            &resources,
        const std::map<std::shared_ptr<wrench::DataFile>,
                       std::vector<std::shared_ptr<wrench::FileLocation>>> &mapping,
        const std::vector<std::shared_ptr<wrench::FileLocation>>
            &previous_allocations,
        unsigned int stripe_count) const {

        // 1. For all services we need to calculate their weight.

        /** TODO: this is probably incorrect  : num_active_service should be the
         * number of ACTIVE targets (i.e. with a current allocation), not the entire
         * number of targets but so far we'll consider that every target is active at
         * any time.
         *
         *  Also /!\ this variable is used throughout the function
         */
        auto active_ost_count = 0;
        for (const auto &resource : resources) {
            active_ost_count += resource.second.size();
        }

        if (active_ost_count == 0) {
            WRENCH_WARN("[lustreWeightedAllocator] active_service_count = 0 - Unable "
                        "to continue");
            throw std::runtime_error(
                "lustreWeightedAllocator: active_service_count cannot be = 0");
        }
        // std::cout << "Active OST count : " << active_ost_count << std::endl;

        uint64_t total_weight = 0;
        std::map<std::shared_ptr<wrench::StorageService>, uint64_t>
            weighted_services = {};

        for (const auto &resource : resources) {

            auto srv_free_space = 0;
            auto srv_free_inodes = 0;
            std::map<std::shared_ptr<wrench::StorageService>, uint64_t>
                weighted_oss_services = {};

            // std::cout << "Computing weights for " << resource.first << std::endl;
            for (const auto &service : resource.second) {

                // std::cout << " - " << service->getName() << " has :" << std::endl;
                auto ss_service =
                    dynamic_pointer_cast<wrench::SimpleStorageService>(service);
                uint64_t current_free_space = ss_service->getTotalFreeSpaceZeroTime();
                // std::cout << "    - " << current_free_space << " free bytes" <<
                // std::endl;
                srv_free_space += (current_free_space >> 16); // Bitshift for overflow
                uint64_t current_files = ss_service->getTotalFilesZeroTime();
                // std::cout << "    - " << current_files << " known files" << std::endl;
                srv_free_inodes += ((this->config->lustre.max_inodes - current_files) >>
                                    8); // Bitshift for overflow, once again

                auto ost_penalty = lustreComputeOstPenalty(
                    current_free_space, current_files, active_ost_count);
                // std::cout << "    - The OST penalty is : " << ost_penalty << std::endl;
                auto weight = lustreComputeOstWeight(current_free_space, current_files,
                                                     ost_penalty);
                // std::cout << "    - The weight is : " << weight << std::endl;

                // At this point we should also take into account for how long the OST has
                // been IDLE. So far we don't because... it's very complex to get it right
                // in the simulation.
                weighted_oss_services[service] = weight;
            }

            // At this point we should also take into account for how long the OSS has
            // been IDLE. So far we don't because... it's very complex to get it right
            // in the simulation.

            // Update all weights with server penalty
            auto oss_penalty =
                lustreComputeOssPenalty(srv_free_space, srv_free_inodes,
                                        weighted_oss_services.size(), resources.size());
            // std::cout << "The full OSS penalty is " << oss_penalty << std::endl;

            for (auto &oss_service : weighted_oss_services) {
                if (oss_service.second < oss_penalty)
                    oss_service.second = 0;
                else
                    oss_service.second -= oss_penalty;

                total_weight += oss_service.second;
            }

            // Update global weight map
            weighted_services.insert(weighted_oss_services.begin(),
                                     weighted_oss_services.end());
        }

        // 2. Pick OST at random, but favor larger weights
        // https://github.com/whamcloud/lustre/blob/a336d7c7c1cd62a5a5213835aa85b8eaa87b076a/lustre/lod/lod_qos.c#L1503

        /** In Lustre, the user provides a striping pattern (stripe size and number of
         * OSTs onto which the stripes will be placed) Here we don't have (nor want)
         * this kind of user input, so we need to derive the stripe size and number of
         * used OSTs from the file size and the total number of OSTs, with some
         * homemade rule.
         */
        auto file_size_b = file->getSize();
        // std::cout << "We need to find storage a file of size " << file_size_b << "
        // bytes" << std::endl; auto stripe_count = std::ceil(file_size_b /
        // this->config->lustre.stripe_size); // Here we potentially ask for more
        // storage than needed due to the rounding

        auto striping =
            lustreComputeStriping(file_size_b, active_ost_count, stripe_count);

        // std::cout << "Stripe size " << striping.stripe_size_b << std::endl;
        // std::cout << "Stripe count " << striping.stripes_count << std::endl;
        // std::cout << "Stripes per ost " << striping.stripes_per_ost << std::endl;
        // std::cout << "TOTAL WEIGHT : " << total_weight << std::endl;

        WRENCH_DEBUG("[lustreWeightedAllocator] LUSTRE WEIGHT ALLOC DEBUG");
        WRENCH_DEBUG("[lustreWeightedAllocator] Stripe count = %ld",
                     striping.stripes_count);
        WRENCH_DEBUG("[lustreWeightedAllocator] Current file_size = %lld",
                     file_size_b);
        WRENCH_DEBUG("[lustreWeightedAllocator] Number of OST / services = %i",
                     active_ost_count);

        // from
        // https://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
        std::random_device rd;
        std::mt19937 gen(rd()); // replace rd() by a static seed for reproducibility
        std::uniform_int_distribution<uint64_t> distrib(0, total_weight);

        std::vector<std::shared_ptr<wrench::StorageService>> selectedOSTs;

        // Let's find enough OSTs for our allocation
        uint64_t nfound = 0;
        while (nfound < striping.stripes_count) {

            uint64_t rand, cur_weight;

            cur_weight = 0;
            rand = distrib(gen);

            auto stripe_idx = 0;
            for (const auto &service : weighted_services) {

                cur_weight += service.second;
                if (cur_weight < rand)
                    continue;

                selectedOSTs.push_back(service.first);
                nfound++;
            }
        }

        // If we could allocate every stripe, return an empty vector that will be
        // interpreted as an allocation failure
        if (selectedOSTs.size() < striping.stripes_count) {
            WRENCH_WARN("[lustreWeightedAllocator] Expected to allocate %ld  stripes, "
                        "but could only allocate %li instead.",
                        striping.stripes_count, selectedOSTs.size());
        }

        return lustreCreateFileParts(file, selectedOSTs, striping.stripe_size_b);
    }
} // namespace fives