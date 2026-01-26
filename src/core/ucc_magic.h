/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_MAGIC_H_
#define UCC_MAGIC_H_

#include "utils/ucc_log.h"
#include "ucc/api/ucc_status.h"

/**
 * @file ucc_magic.h
 * @brief Magic numbers for detecting use-after-free bugs
 *
 * This file contains magic number constants used to detect when applications
 * attempt to use UCC objects (contexts, libraries) after they have been
 * destroyed. This helps prevent use-after-free bugs by marking objects with
 * specific magic values during their lifecycle.
 *
 * See UCC issue #1174 for details.
 */

/* Magic numbers for detecting use-after-free of context */
#define UCC_CONTEXT_MAGIC_VALID         0xC0A7E47DEADBEEF0ULL
#define UCC_CONTEXT_MAGIC_DESTROYED     0xBADC0FFEE0DED000ULL
#define UCC_CONTEXT_MAGIC_UNINITIALIZED 0x0ULL

/* Magic numbers for detecting use-after-free of library */
#define UCC_LIB_MAGIC_VALID             0xFEEDFACEDEADC0DEULL
#define UCC_LIB_MAGIC_DESTROYED         0xDEADBABEC0FFEEE5ULL
#define UCC_LIB_MAGIC_UNINITIALIZED     0x0ULL

/**
 * @brief Check team context validity and return status (macro form)
 *
 * This macro validates the team's context and evaluates to a ucc_status_t value.
 * Use this when you need to capture the status and handle it yourself.
 *
 * Note: This is implemented as a statement expression to allow error logging
 * while still returning a value. It requires GNU C extensions.
 *
 * @param _team  Pointer to ucc_team_t to validate
 * @return UCC_OK if valid, UCC_ERR_INVALID_PARAM if invalid
 */
#define UCC_TEAM_CTX_CHECK_STATUS(_team)                                        \
    ({                                                                          \
        ucc_status_t _status = UCC_OK;                                          \
        if (ucc_unlikely((_team)->contexts == NULL ||                          \
                         (_team)->contexts[0] == NULL)) {                       \
            ucc_error("team %p has NULL context pointer - "                    \
                      "context may have been destroyed", (_team));             \
            _status = UCC_ERR_INVALID_PARAM;                                    \
        } else if (ucc_unlikely((_team)->contexts[0]->magic !=                 \
                                UCC_CONTEXT_MAGIC_VALID)) {                     \
            if ((_team)->contexts[0]->magic == UCC_CONTEXT_MAGIC_DESTROYED) {  \
                ucc_error("team %p uses a destroyed context", (_team));        \
            } else if ((_team)->contexts[0]->magic ==                          \
                       UCC_CONTEXT_MAGIC_UNINITIALIZED) {                      \
                ucc_error("team %p uses an uninitialized context", (_team));   \
            } else {                                                            \
                ucc_error("team %p has corrupted context (magic: 0x%lx)",      \
                          (_team), (unsigned long)(_team)->contexts[0]->magic); \
            }                                                                   \
            _status = UCC_ERR_INVALID_PARAM;                                    \
        }                                                                       \
        _status;                                                                \
    })

/**
 * @brief Validate that a team's context is still valid (statement form)
 *
 * This macro checks that a team's context pointer is non-NULL and that the
 * context has a valid magic number. This prevents use-after-free bugs when
 * applications attempt operations on teams after their contexts have been
 * destroyed via ucc_context_destroy().
 *
 * @param _team  Pointer to ucc_team_t to validate
 * @return UCC_ERR_INVALID_PARAM if validation fails, otherwise continues execution
 *
 * @note This macro returns from the calling function on validation failure
 */
#define UCC_CHECK_TEAM_CTX_VALID(_team)                                         \
    do {                                                                        \
        ucc_status_t _status = UCC_TEAM_CTX_CHECK_STATUS(_team);               \
        if (_status != UCC_OK) {                                                \
            return _status;                                                     \
        }                                                                       \
    } while (0)

/**
 * @brief Check context library validity and return status (macro form)
 *
 * This macro validates the context's library and evaluates to a ucc_status_t value.
 * Use this when you need to capture the status and handle it yourself.
 *
 * @param _ctx  Pointer to ucc_context_t to validate
 * @return UCC_OK if valid, UCC_ERR_INVALID_PARAM if invalid
 */
#define UCC_CTX_LIB_CHECK_STATUS(_ctx)                                          \
    ({                                                                          \
        ucc_status_t _status = UCC_OK;                                          \
        if (ucc_unlikely((_ctx)->lib == NULL)) {                                \
            ucc_error("context %p has NULL library pointer - "                 \
                      "library may have been finalized", (_ctx));               \
            _status = UCC_ERR_INVALID_PARAM;                                    \
        } else if (ucc_unlikely((_ctx)->lib->magic != UCC_LIB_MAGIC_VALID)) {  \
            if ((_ctx)->lib->magic == UCC_LIB_MAGIC_DESTROYED) {               \
                ucc_error("context %p uses a finalized library", (_ctx));      \
            } else if ((_ctx)->lib->magic == UCC_LIB_MAGIC_UNINITIALIZED) {    \
                ucc_error("context %p uses an uninitialized library", (_ctx)); \
            } else {                                                            \
                ucc_error("context %p has corrupted library (magic: 0x%lx)",   \
                          (_ctx), (unsigned long)(_ctx)->lib->magic);           \
            }                                                                   \
            _status = UCC_ERR_INVALID_PARAM;                                    \
        }                                                                       \
        _status;                                                                \
    })

/**
 * @brief Validate that a context's library is still valid (statement form)
 *
 * This macro checks that a context's library pointer is non-NULL and that the
 * library has a valid magic number. This prevents use-after-free bugs when
 * applications attempt operations after the library has been finalized via
 * ucc_finalize().
 *
 * @param _ctx  Pointer to ucc_context_t to validate
 * @return UCC_ERR_INVALID_PARAM if validation fails, otherwise continues execution
 *
 * @note This macro returns from the calling function on validation failure
 */
#define UCC_CHECK_CTX_LIB_VALID(_ctx)                                           \
    do {                                                                        \
        ucc_status_t _status = UCC_CTX_LIB_CHECK_STATUS(_ctx);                 \
        if (_status != UCC_OK) {                                                \
            return _status;                                                     \
        }                                                                       \
    } while (0)

/**
 * @brief Validate that a team's context and library are both still valid
 *
 * This is a convenience macro that combines both context and library validation.
 * Use this when you need to ensure both the context hasn't been destroyed AND
 * the library hasn't been finalized.
 *
 * @param _team  Pointer to ucc_team_t to validate
 * @return UCC_ERR_INVALID_PARAM if validation fails, otherwise continues execution
 *
 * @note This macro returns from the calling function on validation failure
 */
#define UCC_CHECK_TEAM_CTX_AND_LIB_VALID(_team)                                 \
    do {                                                                        \
        UCC_CHECK_TEAM_CTX_VALID(_team);                                        \
        UCC_CHECK_CTX_LIB_VALID((_team)->contexts[0]);                          \
    } while (0)

#endif /* UCC_MAGIC_H_ */
