#ifndef _LOGKV_AUTOSER_PFR_H_
#define _LOGKV_AUTOSER_PFR_H_

#include <boost/pfr.hpp>
#include <logkv/autoser.h>

namespace logkv {

// ----------------------------------------------------------------------------
// All T types that satisfy std::is_aggregate_v<T> (via Boost PFR).
// Excludes std::array and AutoSerializableObject subclasses.
// If including this header, must ensure all other types that have another
// serializer won't match by ensuring they fail std::is_aggregate_v<T>.
// ----------------------------------------------------------------------------

template <typename T> struct is_std_array : std::false_type {};
template <typename T, std::size_t N>
struct is_std_array<std::array<T, N>> : std::true_type {};
template <typename T>
inline constexpr bool is_std_array_v =
  is_std_array<std::remove_cvref_t<T>>::value;

template <typename T>
struct composite_traits<
  T, std::enable_if_t<std::is_aggregate_v<T> && !is_std_array_v<T> &&
                      !std::is_base_of_v<AutoSerializableObject<T>, T>>> {
  using member_types =
    decltype(boost::pfr::structure_to_tuple(std::declval<T>()));
  static auto get_members_by_value(const T& obj) {
    return boost::pfr::structure_to_tuple(obj);
  }
  static auto get_members_by_const_reference(const T& obj) {
    return boost::pfr::structure_tie(obj);
  }
  static auto get_members_by_reference(T& obj) {
    return boost::pfr::structure_tie(obj);
  }
};

} // namespace logkv

#endif