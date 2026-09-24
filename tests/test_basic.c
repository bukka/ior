/* SPDX-License-Identifier: BSD-3-Clause */
#include "test_utils.h"

static void test_init_and_exit(void **state)
{
	(void) state;

	ior_ctx *ctx;
	int ret = ior_queue_init(32, &ctx);
	assert_return_code(ret, 0);
	assert_non_null(ctx);

	ior_queue_exit(ctx);
}

static void test_get_backend_info(void **state)
{
	(void) state;

	ior_ctx *ctx;
	int ret = ior_queue_init(32, &ctx);
	assert_return_code(ret, 0);

	const char *backend = ior_get_backend_name(ctx);
	assert_non_null(backend);
	printf("Backend: %s\n", backend);

	ior_backend_type type = ior_get_backend_type(ctx);
	assert_true(type != IOR_BACKEND_AUTO);

	uint32_t features = ior_get_features(ctx);
	printf("Features: 0x%x\n", features);

	ior_queue_exit(ctx);
}

static void test_sqe_operations(void **state)
{
	test_state *ts = (test_state *) *state;

	ior_sqe *sqe = ior_get_sqe(ts->ctx);
	assert_non_null(sqe);

	// Test prep_nop
	ior_prep_nop(ts->ctx, sqe);
	ior_sqe_set_data(ts->ctx, sqe, (void *) (uintptr_t) 0xDEADBEEF);

	int ret = ior_submit(ts->ctx);
	assert_true(ret >= 0);
}

/*
 * IOR_BACKEND in the environment picks the backend for IOR_BACKEND_AUTO: a
 * name that is unknown, or not built in, fails init with -ENOSYS rather than
 * being ignored; the name of the one in use selects it.
 */
static void test_backend_env(void **state)
{
	(void) state;

	ior_ctx *ctx = NULL;
	assert_return_code(ior_queue_init(32, &ctx), 0);
	const char *name = ior_get_backend_name(ctx);
	ior_backend_type type = ior_get_backend_type(ctx);
	ior_queue_exit(ctx);

	test_setenv("IOR_BACKEND", "no-such-backend");
	ctx = NULL;
	assert_int_equal(ior_queue_init(32, &ctx), -ENOSYS);
	assert_null(ctx);

	test_setenv("IOR_BACKEND", name);
	assert_return_code(ior_queue_init(32, &ctx), 0);
	assert_int_equal(ior_get_backend_type(ctx), type);
	ior_queue_exit(ctx);

	/* An explicit choice is not overridden by the environment. */
	test_setenv("IOR_BACKEND", "no-such-backend");
	ior_params params = { .backend = type };
	assert_return_code(ior_queue_init_params(32, &ctx, &params), 0);
	assert_int_equal(ior_get_backend_type(ctx), type);
	ior_queue_exit(ctx);

	test_setenv("IOR_BACKEND", NULL);
}

int main(void)
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_init_and_exit),
		cmocka_unit_test(test_get_backend_info),
		cmocka_unit_test(test_backend_env),
		cmocka_unit_test_setup_teardown(test_sqe_operations, setup_ior_ctx, teardown_ior_ctx),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
